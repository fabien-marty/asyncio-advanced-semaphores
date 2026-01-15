local key = KEYS[1] -- semaphore redis key (zset)
local now = tonumber(ARGV[1]) -- now timestamp in seconds

-- Clean expired slots
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

return redis.call('ZCARD', key)
