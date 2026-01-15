local waiting_key = KEYS[1] -- waiting key (zset)
local acquisition_id = ARGV[1] -- acquisition id (string)
local heartbeat_max_interval = tonumber(ARGV[2]) -- heartbeat max interval in seconds
local ttl = tonumber(ARGV[3]) -- ttl in seconds
local now = tonumber(ARGV[4]) -- now timestamp in seconds

-- Add the client to the waiting list (NX = only if not exists, preserves queue position on retry)
redis.call('ZADD', waiting_key, 'NX', now + heartbeat_max_interval, acquisition_id)
redis.call('EXPIRE', waiting_key, ttl + 10)

return 0
