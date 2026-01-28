local waiting_key = KEYS[1] -- waiting key (zset)
local waiting_key_heartbeat = KEYS[2] -- waiting key with heartbeat (zset)
local key = KEYS[3] -- semaphore redis key (zset)
local acquisition_id = ARGV[1]
local heartbeat_max_interval = tonumber(ARGV[2]) -- heartbeat max interval in seconds
local ttl = tonumber(ARGV[3]) -- ttl in seconds
local now = tonumber(ARGV[4]) -- now timestamp in seconds

-- Avoid some expirations at Redis level
redis.call('EXPIRE', waiting_key, ttl + 10)
redis.call('EXPIRE', waiting_key_heartbeat, ttl + 10)
redis.call('EXPIRE', key, ttl + 10)

-- XX: Only update elements that already exist. Don't add new elements.
-- CH: Modify the return value from the number of new elements added, to the total number of elements changed (CH is an abbreviation of changed).
--     Changed elements are new elements added and elements already existing for which the score was updated.
--     So elements specified in the command line having the same score as they had in the past are not counted.
--     Note: normally the return value of ZADD only counts the number of new elements added.
local changed_waiting = redis.call('ZADD', waiting_key_heartbeat, 'XX', 'CH', now + heartbeat_max_interval, acquisition_id)
local changed_semaphore = redis.call('ZADD', key, 'XX', 'CH', now + heartbeat_max_interval, acquisition_id)
return {changed_waiting, changed_semaphore}
