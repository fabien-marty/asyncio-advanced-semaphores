local waiting_key_heartbeat = KEYS[1] -- waiting key with heartbeat (zset)
local key = KEYS[2] -- semaphore redis key (zset)
local acquisition_id = ARGV[1]
local heartbeat_max_interval = tonumber(ARGV[2]) -- heartbeat max interval in seconds
local now = tonumber(ARGV[3]) -- now timestamp in seconds

redis.call('ZADD', waiting_key_heartbeat, 'XX', now + heartbeat_max_interval, acquisition_id)
redis.call('ZADD', key, 'XX', now + heartbeat_max_interval, acquisition_id)

return 0
