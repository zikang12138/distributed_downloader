# 分布式下载项目（Distributed Downloader / Caching System）

🧩 一套轻量级的“P2P + Redis 协调”的分布式缓存下载系统，
目标是降低源站压力、提升大文件下载命中率和系统吞吐。

---

## 一、背景与目标

* **场景**：仿真任务中存在大量大文件（场景包、模型包等），Worker 并发下载。
* **问题**：

  * 冷启动时所有节点从源站拉取，容易带宽击穿；
  * 多副本缓存命中率低；
  * 并发无上限，源站压力大；
  * Pod 状态不可见，缺乏自愈。
* **目标**：

  * 本地缓存热点文件，优先命中；
  * Redis 统一注册/心跳；
  * Lua 脚本实现原子限流；
  * 固定 K 个副本预热；
  * Worker 侧简单一致性哈希调度；
  * 减少回源次数，提升整体带宽利用。

---

## 二、系统架构

```
Client(Worker)
   │
   ▼
Download Router → Pod-Cache(N)
                   │
                   ▼
                Origin Server (PFS)
Redis(注册/索引/限流/预热)
```

* **Worker**：请求文件，优先命中副本；失败时回源。
* **Pod**：缓存节点 + 健康上报。
* **Redis**：元信息中心。
* **Origin**：权威文件源。

---

## 三、组件说明

| 模块         | 文件               | 说明                                |
| ---------- | ---------------- | --------------------------------- |
| 源站服务       | origin_server.py | 提供静态文件下载（PFS 模拟）                  |
| 缓存节点       | pod.py           | 本地缓存服务 + 心跳注册 + 预热控制              |
| Worker 客户端 | worker.py        | 下载文件，失败回源，MD5 校验                  |
| 并发控制       | redis_lua.py     | Lua 实现并发占位与释放                     |
| 启动脚本       | run_all.sh       | 一键起 Redis → Origin → Pod → Worker |

---

## 四、关键机制

### 1. 固定 K 个副本预热

* 首次回源仅允许 K 个 Pod；
* 其余 Pod 返回 425（Preheat Required）；
* Redis 维护 `preheat:{file_md5}` 集合（TTL 控制）。

### 2. 一致性哈希调度

* Worker 使用 `md5(path|pod_id)` 排序所有 Pod；
* 同一文件在所有 Worker 眼中优先相同的 Pod；
* Pod 增减时只影响局部文件，负载平稳。

### 3. Redis 键位结构

| Key           | 类型     | 说明                   |
| ------------- | ------ | -------------------- |
| pods:active   | ZSET   | 所有活跃 Pod，score=心跳时间戳 |
| pod:{id}      | HASH   | Pod 元信息              |
| pod:{id}:busy | SET    | 并发占位 token           |
| preheat:{md5} | SET    | 预热授权 Pod 列表          |
| md5:{path}    | STRING | 文件 MD5 校验值           |

---

## 五、本地运行步骤

### 1. 环境准备

```bash
python3 -m venv venv && source venv/bin/activate
pip install redis
docker run -d --name dl-redis -p 6379:6379 redis:7
mkdir -p origin_data pod1_cache pod2_cache downloads
```

### 2. 启动各组件

```bash
# 启动源站
python origin_server.py --port 8000 --root ./origin_data

# 启动两个 Pod
python pod.py --port 9001 --cache-dir ./pod1_cache --origin http://127.0.0.1:8000 --redis-url redis://127.0.0.1:6379/0 --max-conns 2
python pod.py --port 9002 --cache-dir ./pod2_cache --origin http://127.0.0.1:8000 --redis-url redis://127.0.0.1:6379/0 --max-conns 2

# 启动 Worker 下载
python worker.py --path big.pkg --dest ./downloads --origin http://127.0.0.1:8000 --redis-url redis://127.0.0.1:6379/0
```

### 3. 验证与观测

```bash
curl -I http://127.0.0.1:9001/healthz
redis-cli ZRANGE pods:active 0 -1 WITHSCORES
redis-cli SMEMBERS preheat:$(echo -n big.pkg | md5sum | awk '{print $1}')
```

---

## 六、并发与容错测试

```bash
# 模拟多 Worker 并发
for i in $(seq 1 6); do
  python worker.py --path big.pkg --dest ./downloads \
    --origin http://127.0.0.1:8000 \
    --redis-url redis://127.0.0.1:6379/0 &
done
wait
```

观察日志：

* “pod at capacity” → 并发限流触发；
* “425-preheat-required” → 未授权 Pod 拒绝预热；
* 再次下载命中缓存 → 零回源。

---

## 七、关键算法可视化

**一致性哈希（简化）**

```
md5(path|pod1) → 0xAABB
md5(path|pod2) → 0xCCDD
md5(path|pod3) → 0x1122
排序后：pod3 → pod1 → pod2
=> 同一文件始终优先尝试 pod3
```

**固定 K 副本预热**

```
首次下载 big.pkg:
  Worker 选出 {pod1, pod2} 预热
  仅这两台允许回源
  其余 Pod 返回 425，等待同步
```

---

## 八、参数调优

| 参数              | 默认值 | 说明           |
| --------------- | --- | ------------ |
| FRESH_SEC       | 15  | Pod 活跃判定时间窗口 |
| RESERVE_TTL_SEC | 60  | 并发占位 TTL     |
| PREHEAT_K       | 2   | 每文件预热副本数     |
| PREHEAT_TTL_SEC | 300 | 预热授权过期时间     |
| max_conns       | 2   | Pod 并发上限     |

---

## 九、未来扩展方向

* 分块下载 + 断点续传；
* Gateway 层（权重路由 + 熔断）；
* Prometheus 指标观测；
* 灰度策略：新副本小流量验证；
* 跨机房副本权重；
* Lua 脚本预加载与连接池优化。

---

## 十、License

MIT License © 2025
Author: **Zikang Yu**

