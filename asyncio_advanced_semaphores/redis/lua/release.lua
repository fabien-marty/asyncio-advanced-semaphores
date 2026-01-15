local key = KEYS[1] -- semaphore redis key (zset)
local ttl_key = KEYS[2] -- ttl key (zset)
local waiting_key = KEYS[3] -- waiting clients key (zset)
local acquisition_id = ARGV[1] -- acquisition id (string)

redis.call('ZREM', waiting_key, acquisition_id)
redis.call('ZREM', ttl_key, acquisition_id)
local removed = redis.call('ZREM', key, acquisition_id)
return removed
