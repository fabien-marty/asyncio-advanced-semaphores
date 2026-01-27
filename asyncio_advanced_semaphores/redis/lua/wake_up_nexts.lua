local key = KEYS[1] -- semaphore redis key (zset)
local ttl_key = KEYS[2] -- ttl key (zset)
local waiting_key = KEYS[3] -- waiting key (zset)
local waiting_key_heartbeat = KEYS[4] -- waiting key with heartbeat (zset)
local limit = tonumber(ARGV[1]) -- max number of slots
local heartbeat_max_interval = tonumber(ARGV[2]) -- heartbeat max interval in seconds
local ttl = tonumber(ARGV[3]) -- ttl in seconds
local now = tonumber(ARGV[4]) -- now timestamp in seconds
local acquisition_notification_key_pattern = ARGV[5] -- acquisition notification key pattern

-- Clean expired slots (because of TTL)
local removed = redis.call('ZRANGEBYSCORE', ttl_key, '-inf', now, "LIMIT", 0, 10000)
for i = 1, #removed do
    local acquisition_id = removed[i]
    redis.call('ZREM', key, acquisition_id)
    redis.call('ZREM', ttl_key, acquisition_id)
end

-- Clean expired slots (because of heartbeat)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- Check if there is any available slot?
local card = redis.call('ZCARD', key)
if card >= limit then
    -- No available slot => sorry
    return 0
end

-- Clean expired slots in the waiting keys
removed = redis.call('ZRANGEBYSCORE', waiting_key_heartbeat, '-inf', now, "LIMIT", 0, 10000)
for i = 1, #removed do
    local acquisition_id = removed[i]
    redis.call('ZREM', waiting_key_heartbeat, acquisition_id)
    redis.call('ZREM', waiting_key, acquisition_id)
end

local notified = 0
local slots_to_fill = limit - card

-- We have some available slots
-- Try to fill available slots (continue until we've granted enough OR queue is empty)
while notified < slots_to_fill do

    local next_acquisition = redis.call('ZPOPMIN', waiting_key)
    if #next_acquisition == 0 then
        -- No acquisitions waiting
        break
    end
    local next_acquisition_id = next_acquisition[1]

    -- Remove from heartbeat tracking since we're granting the slot
    redis.call('ZREM', waiting_key_heartbeat, next_acquisition_id)

    -- Let's reserve a slot (NX = only if not already holding a slot)
    local added = redis.call('ZADD', key, 'NX', now + heartbeat_max_interval, next_acquisition_id)
    if added == 1 then
        -- New slot was actually granted
        notified = notified + 1
        redis.call('EXPIRE', key, ttl + 10)
    end
    -- Always send notification (even if client already had a slot from retry scenario,
    -- they consumed the previous notification and need a new one to proceed)
    local next_acquisition_list_key = string.gsub(acquisition_notification_key_pattern, "@@@ACQUISITION_ID@@@", next_acquisition_id, 1)
    redis.call('RPUSH', next_acquisition_list_key, '1')
    redis.call('EXPIRE', next_acquisition_list_key, heartbeat_max_interval + 10)

end

return notified
