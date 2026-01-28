local key = KEYS[1] -- semaphore redis key (zset)
local ttl_key = KEYS[2] -- ttl key (zset)
local max_key = KEYS[3] -- max key (string)
local acquisition_id = ARGV[1] -- acquisition id (string)
local limit = tonumber(ARGV[2]) -- max number of slots
local heartbeat_max_interval = tonumber(ARGV[3]) -- heartbeat max interval in seconds
local ttl = tonumber(ARGV[4]) -- ttl in seconds
local now = tonumber(ARGV[5]) -- now timestamp in seconds

-- XX: Only update elements that already exist. Don't add new elements.
-- CH: Modify the return value from the number of new elements added, to the total number of elements changed (CH is an abbreviation of changed).
--     Changed elements are new elements added and elements already existing for which the score was updated.
--     So elements specified in the command line having the same score as they had in the past are not counted.
--     Note: normally the return value of ZADD only counts the number of new elements added.
local changed = redis.call('ZADD', key, 'XX', 'CH', now + heartbeat_max_interval, acquisition_id)
if changed == 1 then
    redis.call('ZADD', ttl_key, now + ttl, acquisition_id)
    redis.call('EXPIRE', ttl_key, ttl + 10)
end
redis.call('SET', max_key, limit, 'EX', ttl + 10)
local card = redis.call('ZCARD', key)

return {changed, card}
