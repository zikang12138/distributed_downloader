import time
import random
from functools import wraps
import redis  # 使用同步 redis 客户端，或替换为 redis.asyncio 版本
from typing import Callable, List


class TokenBucketLimiter:
    def __init__(self, redis_client_provider: Callable[[], redis.Redis]):
        self.redis_client_provider = redis_client_provider
        self._redis = None
        self._script_sha = None
        self._lua_script = self._load_lua_script()

    def _load_lua_script(self) -> str:
        # 假设你将 Lua 脚本放在当前目录下，文件名为 rate_limit.lua
        with open("rate_limit.lua", "r") as f:
            return f.read()

    @property
    def redis(self):
        if self._redis is None:
            self._redis = self.redis_client_provider()
            self._script_sha = self._redis.script_load(self._lua_script)
        return self._redis

    def token_bucket(self, token_type: str, rate: float, capacity: int, param_keys: List[str] = None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # 可扩展支持维度限流：根据传入参数拼接 key
                dim = "_".join([str(kwargs.get(k, '')) for k in (param_keys or [])])
                base_key = f"limiter:{func.__name__}:{token_type}:{dim}"

                keys = [
                    f"{base_key}:tokens",
                    f"{base_key}:timestamp",
                    f"{base_key}:config"
                ]

                while True:
                    result = self.redis.evalsha(
                        self._script_sha,
                        len(keys),
                        *keys,
                        rate,
                        capacity
                    )
                    if result[0] == 1:
                        return func(*args, **kwargs)
                    else:
                        # 添加抖动，防止多个任务同时重试雪崩
                        wait = max(float(result[1]) - time.time(), 0) + random.uniform(0.01, 0.1)
                        time.sleep(wait)
            return wrapper
        return decorator
