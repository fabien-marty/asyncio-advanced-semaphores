local key = KEYS[1] -- semaphore redis key (zset)
local ttl_key = KEYS[2] -- ttl key (zset)
local now = tonumber(ARGV[1]) -- now timestamp in seconds

local removed = redis.call('ZRANGEBYSCORE', ttl_key, '-inf', now, "LIMIT", 0, 10000)

for i = 1, #removed do
    local acquisition_id = removed[i]
    redis.call('ZREM', key, acquisition_id)
    redis.call('ZREM', ttl_key, acquisition_id)
end

return 0
