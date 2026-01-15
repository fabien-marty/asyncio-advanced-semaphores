local key = KEYS[1] -- semaphore redis key (zset)
local waiting_key = KEYS[2] -- waiting key (zset)
local limit = tonumber(ARGV[1]) -- max number of slots
local heartbeat_max_interval = tonumber(ARGV[2]) -- heartbeat max interval in seconds
local ttl = tonumber(ARGV[3]) -- ttl in seconds
local now = tonumber(ARGV[4]) -- now timestamp in seconds
local acquisition_notification_key_pattern = ARGV[5] -- acquisition notification key pattern

-- Clean expired slots
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- Check if there is any available slot?
local card = redis.call('ZCARD', key)
if card >= limit then
    -- No available slot => sorry
    return 0
end

-- Clean expired slots in the waiting key
redis.call('ZREMRANGEBYSCORE', waiting_key, '-inf', now)

local notified = 0

-- We have some available slots
-- Try to fill available slots (limit - card iterations)
for i = 1, limit - card do

    local next_acquisition = redis.call('ZPOPMIN', waiting_key)
    if #next_acquisition == 0 then
        -- No acquisitions waiting
        break
    end
    local next_acquisition_id = next_acquisition[1]
    notified = notified + 1

    -- Let's reserve a slot
    redis.call('ZADD', key, now + heartbeat_max_interval, next_acquisition_id)
    redis.call('EXPIRE', key, ttl + 10)

    -- Let's wake up the acquisition
    local next_acquisition_list_key = string.gsub(acquisition_notification_key_pattern, "@@@ACQUISITION_ID@@@", next_acquisition_id, 1)
    redis.call('RPUSH', next_acquisition_list_key, '1')
    redis.call('EXPIRE', next_acquisition_list_key, heartbeat_max_interval + 10)

end

return notified
