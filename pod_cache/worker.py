# worker.py
# 作用：Worker 负责下载一个逻辑路径（例如 big.pkg）
# 策略：
# 1) 发现“新鲜”Pod（最近心跳） -> 逐个尝试
# 2) 对每个 Pod 先做健康检查（/healthz HEAD）
# 3) 通过 Lua 原子占位（并发上限）成功后才发起下载：GET http://<pod>/files/<path>
# 4) 任一 Pod 成功即返回；如果全部失败或超限，则回退到 origin（直连）
# 5) 下载完成后做 MD5 校验：若 Redis 已有 md5:<path> 则比对；否则写入
#
# 依赖：pip install redis
# 需要与你的 pod.py / origin_server.py 一起使用。

import argparse
import hashlib
import os
import random
import sys
import time
import uuid
import socket
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import redis
from redis_lua import RELEASE_LUA, RESERVE_LUA

# Redis key 约定（与 pod.py 对齐）
PODS_ZSET = "pods:active"
POD_HASH_TMPL = "pod:{id}"
BUSY_SET_TMPL = "pod:{id}:busy"
MD5_KEY_TMPL = "md5:{path}"

# 预热相关
PREHEAT_K = 2
PREHEAT_TTL_SEC = 300
PREHEAT_SET_TMPL = "preheat:{file_md5}"
PREHEAT_LOCK_TMPL = "preheat:{file_md5}:lock"

# “新鲜”心跳阈值（秒内有心跳才算活）
FRESH_SEC = 15
# 并发占位的保护 TTL（秒），防止程序异常导致占位泄漏
RESERVE_TTL_SEC = 60

def http_head(url, timeout=2):
    """发送 HEAD 请求，返回 True/False"""
    try:
        req = Request(url, method="HEAD")
        with urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False
    

def http_get(url, dest_path, timeout=60):
    """用标准库下载到文件（流式），成功 True；失败 False 并删除半拉子文件"""
    try:
        with urlopen(url, timeout=timeout) as resp, open(dest_path, "wb") as out:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        return True, None
    except HTTPError as e:
        # 透传 425 用于上层判断切换 Pod
        if e.code == 425:
            return False, "425-preheat-required"
        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except Exception:
            pass
        return False, f"HTTPError:{e.code}"
    except Exception as e:
        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except Exception:
            pass
        return False, str(e)
    

def compute_md5(path):
    """计算文件 MD5（十六进制字符串）"""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_md5(r, logical_path):
    return r.get(MD5_KEY_TMPL.format(path=logical_path))


def set_md5(r, logical_path, md5_hex):
    r.set(MD5_KEY_TMPL.format(path=logical_path), md5_hex)


def get_fresh_pods(r):
    """读取 pods:active（ZSET），筛选最近 FRESH_SEC 秒内的活跃 pod 列表，打乱顺序"""
    now = time.time()
    pods = []
    for member, score in r.zrange(PODS_ZSET, 0, -1, withscores=True):
        if score >= now - FRESH_SEC:
            pods.append(member)
    random.shuffle(pods)
    return pods

def ring_order_pods(logical_path: str, pods: list[str]) -> list[str]:
    """对 pods 按 (logical_path|pod_id) 的 md5 进行稳定排序（轻量一致性哈希）。"""
    def score(p):
        return hashlib.md5(f"{logical_path}|{p}".encode("utf-8")).hexdigest()
    return sorted(pods, key=score)

def ensure_preheat_set(r, logical_path: str, ordered_pods: list[str]):
    """固定 K 个预热副本：加锁后将前 K 个 Pod 写入 preheat set（带 TTL）。"""
    file_md5 = hashlib.md5(logical_path.encode("utf-8")).hexdigest()
    set_key = PREHEAT_SET_TMPL.format(file_md5=file_md5)
    local_key = PREHEAT_LOCK_TMPL.format(file_md5=file_md5)
    # 若已有集合且数量>=K，则直接返回
    try:
        n = r.scard(set_key)
        if n >= PREHEAT_K:
            return
    except Exception:
        pass
    
    # 分布式互斥，避免并发挑选
    if r.set(local_key, "1", nx=True, ex=10):
        try:
            # 避免重复写
            n2 = r.scard(set_key)
            if n2 is None or n2 < PREHEAT_K:
                targets = ordered_pods[:PREHEAT_K]
                if targets:
                    r.sadd(set_key, *targets)
                    r.expire(set_key, PREHEAT_TTL_SEC)
        finally:
            r.delete(local_key)
    return set_key



def try_download_via_pods(r, logical_path, dest_path):
    """按“健康 -> 占位 -> 下载”的流程尝试所有“新鲜 pod”"""
    pods = get_fresh_pods(r)
    if not pods:
        print("[worker] no fresh pods available")
        return False, "no pods"
    
    # 一致性哈希
    pods = ring_order_pods(logical_path, pods)

    # 预加载 Lua 脚本
    reserve_sha = r.script_load(RESERVE_LUA)
    release_sha = r.script_load(RELEASE_LUA)

    # 固定 K 个预热副本
    ensure_preheat_set(r, logical_path, pods)

    hostname = socket.gethostname()

    for pod_id in pods:
        h = r.hgetall(POD_HASH_TMPL.format(id=pod_id))
        if not h:
            # ZSET 里挂了旧 member，清理一下
            r.zrem(PODS_ZSET, pod_id)
            continue        
        
        host = h.get("host")
        port = h.get("port")
        max_conns = int(h.get("max_conns", 1))

        # 健康检查
        health_url = f"http://{host}:{port}/healthz"
        if not http_head(health_url):
            print(f"[worker] pod {pod_id} unhealthy; removing")
            r.zrem(PODS_ZSET, pod_id)
            r.delete(POD_HASH_TMPL.format(id=pod_id))
            continue

        # 并发占位
        busy_key = BUSY_SET_TMPL.format(id=pod_id)
        token = f"{hostname}:{uuid.uuid4()}"
        ok = r.evalsha(reserve_sha, 1, busy_key, str(max_conns), token, str(RESERVE_TTL_SEC))
        if ok != 1:
            print(f"[worker] pod {pod_id} at capacity")
            continue
        
        try:
            # 真正下载
            url = f"http://{host}:{port}/files/{logical_path}"
            print(f"[worker] downloading via pod {pod_id} -> {url}")
            success, err = http_get(url, dest_path)
            if success:
                return True, None
            else:
                if err == "425-preheat-required":
                    print(f"[worker] pod {pod_id} not authorized to preheat; try next")
                else:
                    print(f"[worker] error via pod {pod_id}: {err}")
        finally:
            # 释放占位
            r.evalsha(release_sha, 1, busy_key, token)

    return False, "all pods failed or busy"


def download_fallback(origin, logical_path, dest_path):
    """全部 pod 失败/繁忙时，直连源站"""
    url = f"{origin.rstrip('/')}/{logical_path}"
    print(f"[worker] fallback to origin {url}")
    return http_get(url, dest_path)


def main():
    parser = argparse.ArgumentParser(description="Worker that downloads via P2P pods with Redis-based scheduling")
    parser.add_argument("--path", required=True, help="逻辑路径，如 big.pkg 或 packages/a/b/c.whl")
    parser.add_argument("--dest", default="./downloads", help="下载保存目录")
    parser.add_argument("--origin", required=True, help="origin 基地址，如 http://127.0.0.1:8000")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    args = parser.parse_args()

    os.makedirs(args.dest, exist_ok=True)
    out_path = os.path.join(args.dest, os.path.basename(args.path))

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    # 1) 优先通过 Pod 下载
    ok, err = try_download_via_pods(r, args.path, out_path)
    if not ok:
        # 2) 失败则回退源站
        ok2, err2 = download_fallback(args.origin, args.path, out_path)
        if not ok2:
            print(f"[worker] failed: {err} / {err2}")
            sys.exit(2)

    # 3) MD5 校验（首次下载写入，后续对比）
    known_md5 = fetch_md5(r, args.path)
    got_md5 = compute_md5(out_path)
    if known_md5:
        if known_md5 != got_md5:
            print(f"[worker] MD5 mismatch! expected={known_md5}, got={got_md5}")
            sys.exit(3)
        else:
            print(f"[worker] MD5 OK: {got_md5}")
    else:
        set_md5(r, args.path, got_md5)
        print(f"[worker] MD5 set: {got_md5}")

    print(f"[worker] done: {out_path}")


if __name__ == "__main__":
    main()