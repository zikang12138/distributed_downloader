# pod.py
# 作用：Pod 节点，既能充当缓存服务器，又能把自己的元数据注册到 Redis，供 Worker 发现。
# 功能：
# 1. 对外提供 /files/<path>：首次未命中 -> 去 origin 拉取并缓存，后续直接本地返回
# 2. 对外提供 /healthz：健康探针接口
# 3. 定时心跳写入 Redis，维持活跃状态
# 4. 退出时清理 Redis 状态，避免“僵尸节点”

import argparse
import hashlib
import atexit
import functools
import http.server
import os
import re
import signal
import socket
import socketserver
import sys
import threading
import time
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

import redis  # pip install redis

PODS_ZSET = "pods:active"  # ZSET：所有活跃 pod 节点，score=最近心跳时间戳
POD_HASH_TMPL = "pod:{id}"   # HASH：pod 的详细信息，如 host/port/cache_dir/origin
BUSY_SET_TMPL = "pod:{id}:busy"   # SET：并发连接占位（此处暂未使用，留给 Worker 控制并发）

HEARTBEAT_SEC = 5   # 心跳刷新间隔，秒

SAFE_PATH = re.compile(r"^[A-Za-z0-9._/\-]+$")   # 限制文件名字符，防止目录穿越

class CacheRequestHandler(http.server.SimpleHTTPRequestHandler):
    """
    HTTP 请求处理器，继承自标准库的 SimpleHTTPRequestHandler。
    我们覆盖 do_GET 和 do_HEAD，实现缓存逻辑和健康探针。
    """

    def __init__(self, *args, cache_dir=None, origin=None, redis_cli=None, pod_id=None, **kwargs):
        self.cache_dir = cache_dir  # 本地缓存目录
        self.origin = origin.rstrip("/")# 源站 base URL
        self.redis = redis_cli
        self.pod_id = pod_id
        # 父类会根据 self.path 去 self.directory 下找文件
        super().__init__(*args, directory=cache_dir, **kwargs)

    def log_message(self, format, *args):
        # 打印日志，加上 [pod] 前缀方便区分
        sys.stderr.write("[pod] " + (format % args) + "\n")

    def _file_md5(self, rel: str) -> bool:
        return hashlib.md5(rel.encode("utf-8")).hexdigest()
    
    def _ensure_cached(self, rel: str) -> bool:
        """
        确保 cache_dir/<rel> 存在；如不存在则从 origin 下沉。
        成功返回 True，失败返回 False（并已写 502 响应）。
        """
        local_path = os.path.join(self.cache_dir, rel)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if os.path.exists(local_path):
            return True
        # 固定 K 副本预热：非授权 Pod 避免回源
        file_key = self._file_md5(rel)
        preheat_set = f"preheat:{file_key}"
        try:
            if self.redis is not None and self.pod_id is not None:
                allowed = self.redis.sismember(preheat_set, self.pod_id)
                if not allowed:
                    # 返回 425 Too Early（自定义信号），提示 Worker 切换到授权 Pod
                    self.send_response(425, "Preheat Required")
                    self.send_header("Content-Type", "application/json")
                    self.send_header("X-Preheat-Needed", "1")
                    self.end_headers()
                    self.wfile.write(b'{"error":"preheat required"}')
                    return False
        except Exception as e:
            # Redis 不可用时退化为直接回源，保证可用性
            self.log_message("preheat check error: %s", e)

        origin_url = f"{self.origin}/{rel}"
        self.log_message("fetching from origin: %s", origin_url)
        try:
            # 从 origin 拉取文件，流式写入
            with urlopen(origin_url, timeout=30) as resp, open(local_path, "wb") as out:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            self.log_message("cached %s from origin", rel)
            return True
        except Exception as e:
            # 拉取失败 -> 返回 502，并删除半拉子文件
            print(f"[pod] origin fetch failed: {self.origin}/{rel} -> {e}", file=sys.stderr)
            self.send_error(502, f"Failed to fetch from origin: {e}")
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except Exception:
                pass
            return False

    def _parse_rel(self):
        """
        从原始 path 中解析出 /files/<rel> 的 rel。
        返回 (is_files_path: bool, rel: str or None)
        """
        parsed = urlparse(self.path)
        raw_path = parsed.path
        self.log_message("raw path: %r", raw_path)

        if not raw_path.startswith("/files"):
            return False, None

        parts = raw_path.split("/", 2)  # ['', 'files', '<rel>']
        rel = parts[2] if len(parts) > 2 else ""
        rel = unquote(rel).lstrip("/")
        self.log_message("rel parsed: %r", rel)

        if not rel or not SAFE_PATH.match(rel):
            self.send_error(400, f"bad path: {rel!r}")
            return True, None

        return True, rel

    # 让 HEAD /healthz 返回 200；/files/<rel> 也要走缓存与路径重写
    def do_HEAD(self):
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            # 健康检查：直接返回 200
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            return

        is_files, rel = self._parse_rel()
        if is_files:
            if rel is None:
                return  # 已返回错误码

            # 确保缓存存在（HEAD 也触发下沉，便于你用 curl -I 验证）
            if not self._ensure_cached(rel):
                return  # 已 502

            # 核心修复：重写路径为 "/<rel>"，让父类在 cache_dir 下找
            self.path = "/" + rel
            return http.server.SimpleHTTPRequestHandler.do_HEAD(self)

        # 其他路径交给父类（通常会 404）
        return http.server.SimpleHTTPRequestHandler.do_HEAD(self)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok": true}')
            return

        is_files, rel = self._parse_rel()
        if is_files:
            if rel is None:
                return  # 已返回错误码

            # 确保缓存存在（首次 GET 触发下沉）
            if not self._ensure_cached(rel):
                return  # 已 502

            # 核心修复：重写路径为 "/<rel>"，让父类在 cache_dir 下找
            self.path = "/" + rel
            return http.server.SimpleHTTPRequestHandler.do_GET(self)

        self.send_error(404, "Not Found")

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def register_redis(r, pod_id, info):
    """注册 pod 信息到 Redis"""
    pipe = r.pipeline()
    pipe.hset(POD_HASH_TMPL.format(id=pod_id), mapping=info)
    pipe.zadd(PODS_ZSET, {pod_id: time.time()})
    pipe.execute()

def heartbeat_loop(r, pod_id, info, stop_evt):
    """后台线程：定时刷新心跳到 Redis"""
    while not stop_evt.is_set():
        try:
            info["last_seen"] = str(time.time())
            r.hset(POD_HASH_TMPL.format(id=pod_id), mapping=info)
            r.zadd(PODS_ZSET, {pod_id: time.time()})
        except Exception as e:
            print(f"[pod] heartbeat error: {e}", file=sys.stderr)
        stop_evt.wait(HEARTBEAT_SEC)

def unregister(r, pod_id):
    """Pod 下线时清理 Redis 状态"""
    try:
        r.zrem(PODS_ZSET, pod_id)
        r.delete(BUSY_SET_TMPL.format(id=pod_id))
        r.delete(POD_HASH_TMPL.format(id=pod_id))
        print(f"[pod] unregistered {pod_id}")
    except Exception as e:
        print(f"[pod] unregister error: {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Pod cache node")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--cache-dir", type=str, required=True)
    parser.add_argument("--origin", type=str, required=True)
    parser.add_argument("--redis-url", type=str, default="redis://127.0.0.1:6379/0")
    parser.add_argument("--max-conns", type=int, default=2)
    args = parser.parse_args()

    os.makedirs(args.cache_dir, exist_ok=True)
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    host_ip = get_ip()
    pod_id = f"{host_ip}:{args.port}"

    info = {
        "host": host_ip,
        "port": str(args.port),
        "cache_dir": os.path.abspath(args.cache_dir),
        "origin": args.origin,
        "max_conns": str(args.max_conns),
        "last_seen": str(time.time()),
    }
    register_redis(r, pod_id, info)

    stop_evt = threading.Event()
    t = threading.Thread(target=heartbeat_loop, args=(r, pod_id, info, stop_evt), daemon=True)
    t.start()

    def _cleanup(*_):
        stop_evt.set()
        unregister(r, pod_id)
        sys.exit(0)

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)
    atexit.register(unregister, r, pod_id)

    handler = functools.partial(CacheRequestHandler, cache_dir=args.cache_dir, origin=args.origin, redis_cli=r, pod_id=pod_id)
    with socketserver.ThreadingTCPServer(("0.0.0.0", args.port), handler) as httpd:
        print(f"[pod] {pod_id} serving cache at {args.cache_dir}, origin={args.origin}, max_conns={args.max_conns}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            _cleanup()

if __name__ == "__main__":
    main()
