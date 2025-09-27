local tokens_key = KEYS[1]
local timestamp_key = KEYS[2]
local config_key = KEYS[3]
local requested = 1

-- 获取配置
local config = redis.call('HGETALL', config_key)
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
if next(config) ~= nil then
    for i=1,#config,2 do
        if config[i] == 'rate' then
            rate = tonumber(config[i+1])
        elseif config[i] == 'capacity' then
            capacity = tonumber(config[i+1])
        end
    end
end

-- 当前时间
local now = redis.call('TIME')[1]
local current_tokens = tonumber(redis.call('GET', tokens_key))
local exists = redis.call('EXISTS', tokens_key)
if exists == 0 then
    redis.call('SET', tokens_key, capacity)
    redis.call('SET', timestamp_key, now)
    current_tokens = capacity
else
    local last_time = redis.call('GET', timestamp_key) or now
    last_time = tonumber(last_time)
    local delta = math.max(now - last_time, 0)
    local new_tokens = delta * rate
    current_tokens = math.min(current_tokens + new_tokens, capacity)
    if new_tokens > 0 then
        redis.call('SET', tokens_key, current_tokens)
        redis.call('SET', timestamp_key, now)
    end
end

-- 尝试扣除令牌
if current_tokens >= requested then
    redis.call('DECRBY', tokens_key, requested)
    return {1}
else
    local deficit = requested - current_tokens
    local wait_time = deficit / rate
    return {0, tostring(now + wait_time)}
end