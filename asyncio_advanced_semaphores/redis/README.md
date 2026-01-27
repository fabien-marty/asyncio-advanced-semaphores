# RedisSemaphore Implementation

This document describes the internal architecture and implementation of `RedisSemaphore`, a distributed semaphore backed by Redis.

For details on Redis keys structure and Lua scripts, see [lua/README.md](lua/README.md).

## Overview

`RedisSemaphore` provides a distributed counting semaphore that works across multiple processes and machines. It inherits from the abstract `Semaphore` class and implements all required methods using Redis as the coordination backend.

Key features:
- **Distributed**: Works across multiple processes/machines via Redis
- **Fair queuing**: FIFO ordering for waiting clients
- **Automatic cleanup**: Dead clients are detected and their slots released
- **TTL enforcement**: Maximum hold time can be enforced
- **Resilient**: Retry logic with exponential backoff for Redis operations

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           RedisSemaphore                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────┐  ┌─────────────────────────┐                   │
│  │   RedisClientManager    │  │   _ping_tasks           │                   │
│  │   (connection pools)    │  │   (heartbeat tasks)     │                   │
│  └───────────┬─────────────┘  └───────────┬─────────────┘                   │
│              │                            │                                 │
│              ▼                            ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         Redis (Lua Scripts)                         │    │
│  │   queue.lua │ wake_up_nexts.lua │ acquire.lua │ release.lua │ etc.  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Notes:
- TTL cleanup is performed inline within the Lua scripts (`wake_up_nexts.lua` and `card.lua`) rather than by a separate background task. This simplifies the architecture while maintaining correctness.
- The waiting queue uses two separate Redis keys: one for FIFO ordering (insertion time) and one for liveness detection (heartbeat expiration). This ensures strict fair queuing where heartbeat refreshes do not affect queue position.

## Components

### RedisSemaphore (`sem.py`)

The main semaphore class with the following configuration:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Semaphore name. Same name = shared semaphore |
| `value` | `int` | required | Number of available slots |
| `max_acquire_time` | `float \| None` | `None` | Maximum time to wait for acquisition |
| `ttl` | `int \| None` | `None` | Maximum hold time after acquisition |
| `heartbeat_max_interval` | `int \| None` | `180` | Max seconds between heartbeats before considered dead |
| `config` | `RedisConfig` | default instance | Redis configuration |

### RedisClientManager (`client.py`)

Manages Redis connections with separate pools for acquire and release operations:

- **Dual connection pools**: Separates acquire operations (which may block via `BLPOP`) from release operations to prevent deadlocks
- **Event loop binding**: Validates that the same event loop is used consistently
- **Lazy initialization**: Pools are created on first use

### RedisConfig (`conf.py`)

Configuration for Redis connections:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `namespace` | `str` | `"adv-sem"` | Prefix for all Redis keys |
| `url` | `str` | `"redis://localhost:6379"` | Redis connection URL |
| `max_connections` | `int` | `300` | Maximum pool connections |
| `socket_timeout` | `int` | `30` | Socket operation timeout |
| `socket_connect_timeout` | `int` | `10` | Connection establishment timeout |
| `number_of_attempts` | `int` | `3` | Retry attempts for Redis operations |
| `retry_multiplier` | `float` | `2` | Exponential backoff multiplier |
| `retry_min_delay` | `float` | `1` | Minimum retry delay (seconds) |
| `retry_max_delay` | `float` | `60` | Maximum retry delay (seconds) |

## Acquisition Flow (Python Side)

```python
async def acquire(self) -> AcquisitionResult:
    # 1. Generate unique acquisition_id
    acquisition_id = uuid.uuid4().hex
    
    # 2. Create heartbeat task
    self._create_ping_task(acquisition_id)
    
    # 3. Queue: Add to waiting queue (queue.lua)
    await queue_script(...)
    
    # 4. Wait loop
    while True:
        # Wake up next clients if slots available (wake_up_nexts.lua)
        # Note: This script also cleans up expired TTL slots inline
        await wake_up_nexts_script(...)
        
        # Wait for notification (BLPOP/LPOP on notification list)
        notified = await client.blpop(notification_key, timeout=1)
        if notified:
            break
    
    # 5. Confirm acquisition (acquire.lua)
    # Returns {changed, card} where card is the current slot count
    changed, card = await acquire_script(...)
    
    # 6. Log acquisition (name, acquire_time, slot_number, max_slots, type)
    
    return AcquisitionResult(acquisition_id=acquisition_id, slot_number=card)
```

### Notification Mechanism

The acquire loop uses a hybrid polling strategy:

1. **Long polling phase** (first 100 seconds): Uses `BLPOP` with 1-second timeout for efficient waiting
2. **Short polling phase** (after 100 seconds): Switches to `LPOP` + `asyncio.sleep` to avoid Redis connection issues with very long waits

### Error Handling

If any exception occurs during acquisition (including `asyncio.CancelledError`):
1. The ping task is cancelled
2. The `release.lua` script removes the client from all Redis keys
3. This cleanup runs in a "super shield" to ensure it completes even if the task is cancelled

## Release Flow (Python Side)

```python
async def release(self, acquisition_id: str) -> None:
    # 1. Cancel heartbeat task
    self._cancel_ping_task(acquisition_id)
    
    # 2. Remove from Redis (release.lua)
    await release_script(...)
    
    # 3. Log release (name, type)
```

The release is wrapped in a "super shield" task to ensure it completes even during cancellation.

## Acquisition ID Tracking

Each acquisition is tracked with a unique `acquisition_id`:

- Generated as a UUID when `acquire()` is called
- Returned to the caller as part of the `AcquisitionResult` object
- Must be passed to `release()` to release the specific slot
- Enables precise tracking and release of individual acquisitions

## Retry Logic

All Redis operations use `tenacity` for retry with exponential backoff:

```python
AsyncRetrying(
    stop=stop_after_attempt(number_of_attempts),
    wait=wait_exponential(
        multiplier=retry_multiplier,
        min=retry_min_delay,
        max=retry_max_delay
    ),
    retry=retry_if_exception_type(),  # retry on any exception
    reraise=True
)
```

## Class Methods

### `get_acquired_stats()`

Returns current usage statistics for semaphores:

```python
stats = await RedisSemaphore.get_acquired_stats(
    names=["my-sem"],  # or None for all
    limit=100,
    client_manager=my_manager
)
# Returns: {"my-sem": SemaphoreStats(acquired_slots=5, max_slots=10)}
```

Uses Redis `SCAN` to find semaphore keys and pipelines for efficient batch queries.

## Thread Safety

- Each `RedisClientManager` is bound to a single event loop (validated on use)
- Multiple `RedisSemaphore` instances can share a `RedisClientManager`

## Usage Example

```python
from asyncio_advanced_semaphores import RedisSemaphore, RedisConfig

# Configure Redis connection
config = RedisConfig(
    url="redis://localhost:6379",
    namespace="my-app"
)

# Create semaphore
sem = RedisSemaphore(
    name="my-resource",
    value=5,
    ttl=300,  # 5 minutes max hold time
    max_acquire_time=60,  # 1 minute max wait
    config=config
)

# Use as context manager (recommended)
async with sem.cm():
    # Do work with acquired slot
    pass

# Or manual acquire/release
result = await sem.acquire()
try:
    # Do work
    pass
finally:
    await sem.release(result.acquisition_id)
```

