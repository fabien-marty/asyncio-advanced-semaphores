local key = KEYS[1] -- semaphore redis key (zset)
local ttl_key = KEYS[2] -- ttl key (zset)
local max_key = KEYS[3] -- max key (string)
local acquisition_id = ARGV[1] -- acquisition id (string)
local limit = tonumber(ARGV[2]) -- max number of slots
local heartbeat_max_interval = tonumber(ARGV[3]) -- heartbeat max interval in seconds
local ttl = tonumber(ARGV[4]) -- ttl in seconds
local now = tonumber(ARGV[5]) -- now timestamp in seconds

local changed = redis.call('ZADD', key, 'XX', 'CH', now + heartbeat_max_interval, acquisition_id)
if changed == 1 then
    redis.call('ZADD', ttl_key, now + ttl, acquisition_id)
    redis.call('EXPIRE', ttl_key, ttl + 10)
end
redis.call('SET', max_key, limit, 'EX', ttl + 10)

return changed
