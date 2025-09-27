# redis_lua.py
# 用 Redis 的 SET 结构实现“并发占位”：
# - 预定：SCARD < limit 才允许 SADD 一个唯一 token，并给 SET 设置 TTL（防泄漏）
# - 释放：SREM token
#
# KEYS[1] = busy_set_key     (例如 "pod:172.18.153.153:9001:busy")
# ARGV[1] = limit            (每个 pod 的并发上限，来自 pod.max_conns)
# ARGV[2] = token            (唯一标识本次占位，例如 "<hostname>:<uuid>")
# ARGV[3] = ttl_seconds      (保护超时清理的 TTL，避免程序异常导致占位泄漏)

RESERVE_LUA = """
local n = redis.call('SCARD', KEYS[1])
if n < tonumber(ARGV[1]) then
  redis.call('SADD', KEYS[1], ARGV[2])
  if tonumber(ARGV[3]) > 0 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
  end
  return 1
else
  return 0
end
"""


RELEASE_LUA = """
redis.call('SREM', KEYS[1], ARGV[1])
return redis.call('SCARD', KEYS[1])
"""