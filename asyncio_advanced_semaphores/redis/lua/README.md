# Redis Lua Scripts for RedisSemaphore

This document describes the Redis keys structure and the algorithm used by the `RedisSemaphore` implementation.

## Redis Keys

All keys are prefixed with a configurable namespace (default: `adv-sem`).

| Key Pattern | Type | Description |
|-------------|------|-------------|
| `{namespace}:semaphore_main:{name}` | ZSET | Main semaphore key. Members are `acquisition_id`, scores are expiration timestamps (now + heartbeat_max_interval). Expired members indicate dead clients. |
| `{namespace}:semaphore_ttl:{name}` | ZSET | TTL tracking. Members are `acquisition_id`, scores are absolute TTL expiration timestamps. Used to enforce maximum hold time. |
| `{namespace}:semaphore_max:{name}` | STRING | Stores the maximum number of slots for this semaphore. Used for statistics. |
| `{namespace}:semaphore_waiting:{name}` | ZSET | Waiting queue. Members are `acquisition_id`, scores are expiration timestamps. FIFO order is preserved by score (insertion time + heartbeat). |
| `{namespace}:acquisition_notification:{acquisition_id}` | LIST | Per-acquisition notification channel. Used to wake up a specific waiting client when a slot becomes available. |

## Algorithm Overview

### Acquisition Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ACQUIRE FLOW                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Client generates unique acquisition_id                                  │
│  2. Start ping task (heartbeat)                                             │
│  3. QUEUE: Add to waiting queue (semaphore_waiting)                         │
│  4. WAKE_UP_NEXTS: Check for available slots and notify waiting clients     │
│  5. BLPOP/LPOP: Wait for notification on acquisition_notification list      │
│  6. ACQUIRE: Confirm acquisition and update TTL tracking                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Release Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              RELEASE FLOW                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Cancel ping task                                                        │
│  2. RELEASE: Remove acquisition_id from all keys (main, ttl, waiting)       │
│  3. Next acquire iteration will call WAKE_UP_NEXTS to fill freed slot       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Liveness Detection

- **Heartbeat (ping)**: Clients periodically refresh their score in both `semaphore_waiting` and `semaphore_main` keys
- **Expiration**: If a client fails to heartbeat within `heartbeat_max_interval`, its score becomes < now and it's considered expired
- **Cleanup**: Expired entries are removed via `ZREMRANGEBYSCORE key -inf now`

### TTL Enforcement

- The `semaphore_ttl` ZSET tracks absolute expiration times for acquisitions
- TTL cleanup is performed inline within `wake_up_nexts.lua` and `card.lua` scripts
- Expired acquisitions (score < now) are removed from both semaphore and TTL keys
- This ensures slots are freed even if a client holds them beyond the configured TTL

## Lua Scripts

### `queue.lua`

Adds a client to the waiting queue.

**Keys:**
- `KEYS[1]`: waiting key (ZSET)

**Args:**
- `ARGV[1]`: acquisition_id
- `ARGV[2]`: heartbeat_max_interval (seconds)
- `ARGV[3]`: ttl (seconds)
- `ARGV[4]`: now (timestamp)

**Behavior:**
- Adds acquisition_id to waiting queue with score = now + heartbeat_max_interval
- Sets expiry on the key

### `wake_up_nexts.lua`

Checks for available slots and wakes up waiting clients.

**Keys:**
- `KEYS[1]`: semaphore key (ZSET)
- `KEYS[2]`: ttl key (ZSET)
- `KEYS[3]`: waiting key (ZSET)

**Args:**
- `ARGV[1]`: limit (max slots)
- `ARGV[2]`: heartbeat_max_interval (seconds)
- `ARGV[3]`: ttl (seconds)
- `ARGV[4]`: now (timestamp)
- `ARGV[5]`: acquisition_notification_key_pattern (with `@@@ACQUISITION_ID@@@` placeholder)

**Behavior:**
1. Clean expired slots from TTL key (removes from both semaphore and TTL keys)
2. Clean expired slots from semaphore key (heartbeat expiration)
3. Check available slots (limit - current count)
4. Clean expired entries from waiting queue
5. For each available slot:
   - Pop oldest waiting client (ZPOPMIN - FIFO)
   - Reserve slot in semaphore key
   - Notify client via RPUSH to notification list

**Returns:** Number of clients notified

### `acquire.lua`

Confirms an acquisition after being notified.

**Keys:**
- `KEYS[1]`: semaphore key (ZSET)
- `KEYS[2]`: ttl key (ZSET)
- `KEYS[3]`: max key (STRING)

**Args:**
- `ARGV[1]`: acquisition_id
- `ARGV[2]`: limit (max slots)
- `ARGV[3]`: heartbeat_max_interval (seconds)
- `ARGV[4]`: ttl (seconds)
- `ARGV[5]`: now (timestamp)

**Behavior:**
- Updates existing entry with `ZADD XX CH` (only updates, doesn't insert)
- If updated, also updates TTL tracking
- Stores max limit for statistics

**Returns:** `{changed, card}` where:
- `changed`: 1 if successfully updated, 0 otherwise (indicates stale acquisition)
- `card`: current number of acquired slots (ZCARD of semaphore key)

### `release.lua`

Releases an acquired slot.

**Keys:**
- `KEYS[1]`: semaphore key (ZSET)
- `KEYS[2]`: ttl key (ZSET)
- `KEYS[3]`: waiting key (ZSET)

**Args:**
- `ARGV[1]`: acquisition_id

**Behavior:**
- Removes acquisition_id from all three keys

**Returns:** Number of elements removed from semaphore key (0 or 1)

### `ping.lua`

Refreshes heartbeat for a client (in waiting queue or holding a slot).

**Keys:**
- `KEYS[1]`: waiting key (ZSET)
- `KEYS[2]`: semaphore key (ZSET)

**Args:**
- `ARGV[1]`: acquisition_id
- `ARGV[2]`: heartbeat_max_interval (seconds)
- `ARGV[3]`: now (timestamp)

**Behavior:**
- Updates score with `ZADD XX` (only if exists) in both keys

### `card.lua`

Returns the current count of active slots.

**Keys:**
- `KEYS[1]`: semaphore key (ZSET)
- `KEYS[2]`: ttl key (ZSET)

**Args:**
- `ARGV[1]`: now (timestamp)

**Behavior:**
- Cleans expired slots from TTL key (removes from both semaphore and TTL keys)
- Cleans expired slots from semaphore key (heartbeat expiration)
- Returns ZCARD

## Atomicity

All scripts run atomically in Redis (single-threaded execution). This guarantees:
- No race conditions between slot counting and allocation
- FIFO ordering is preserved
- Heartbeat updates are atomic

## Fairness

The waiting queue uses ZSET with insertion timestamp as score, combined with `ZPOPMIN` to ensure FIFO ordering. First client to queue gets the first available slot.

