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
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐ │
│  │ RedisClientManager │  │  _PingTasksManager │  │     _TTLChecker           │ │
│  │ (connection pools) │  │ (heartbeat tasks)  │  │ (background cleanup)      │ │
│  └────────┬────────┘  └────────┬────────┘  └──────────┬──────────────────┘ │
│           │                    │                      │                     │
│           ▼                    ▼                      ▼                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Redis (Lua Scripts)                          │   │
│  │   queue.lua │ wake_up_nexts.lua │ acquire.lua │ release.lua │ etc.  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### RedisSemaphore (`sem.py`)

The main semaphore class with the following configuration:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | random UUID | Semaphore name. Same name = shared semaphore |
| `value` | `int` | required | Number of available slots |
| `max_acquire_time` | `float \| None` | `None` | Maximum time to wait for acquisition |
| `ttl` | `int \| None` | `None` | Maximum hold time after acquisition |
| `heartbeat_max_interval` | `int` | `10` | Max seconds between heartbeats before considered dead |
| `client_manager` | `RedisClientManager` | default instance | Redis connection manager |

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

### _PingTasksManager (`ping.py`)

Manages heartbeat tasks for active acquisitions:

- Creates a background asyncio task per acquisition
- Periodically calls `ping.lua` to refresh the client's score in Redis
- Tasks are cancelled on release
- Thread-safe management of task registry

### _TTLChecker (`ttl.py`)

Background task that enforces TTL limits:

- One checker per `RedisConfig` (shared across semaphores)
- Periodically calls `clean_ttl.lua` to remove expired acquisitions
- Only runs when there are active acquisitions (tracked via `_PingTasksManager`)

## Acquisition Flow (Python Side)

```python
async def acquire(self) -> SemaphoreAcquisition:
    # 1. Generate unique acquisition_id
    acquisition_id = _new_acquisition_id()
    
    # 2. Start TTL checker if not running
    _start_ttl_checker_if_needed(self.client_manager)
    
    # 3. Create heartbeat task
    self._create_ping_task(acquisition_id)
    
    # 4. Queue: Add to waiting queue (queue.lua)
    await queue_script(...)
    
    # 5. Wait loop
    while True:
        # Wake up next clients if slots available (wake_up_nexts.lua)
        await wake_up_nexts_script(...)
        
        # Wait for notification (BLPOP/LPOP on notification list)
        notified = await client.blpop(notification_key, timeout=1)
        if notified:
            break
    
    # 6. Confirm acquisition (acquire.lua)
    await acquire_script(...)
    
    # 7. Track acquisition ID for this task
    _push_acquisition_id(name, acquisition_id)
    
    return SemaphoreAcquisition(id=acquisition_id, acquire_time=...)
```

### Notification Mechanism

The acquire loop uses a hybrid polling strategy:

1. **Long polling phase** (first 60 seconds): Uses `BLPOP` with 1-second timeout for efficient waiting
2. **Short polling phase** (after 60 seconds): Switches to `LPOP` + `asyncio.sleep` to avoid Redis connection issues with very long waits

### Error Handling

If any exception occurs during acquisition (including `asyncio.CancelledError`):
1. The ping task is cancelled
2. The `release.lua` script removes the client from all Redis keys
3. This cleanup runs in a "super shield" to ensure it completes even if the task is cancelled

## Release Flow (Python Side)

```python
async def arelease(self, acquisition_id: str | None = None) -> None:
    # 1. Get acquisition_id from tracker if not provided
    if acquisition_id is None:
        acquisition_id = _pop_acquisition_id(self.name)
    
    # 2. Cancel heartbeat task
    _PING_TASKS_MANAGER.cancel_task(acquisition_id)
    
    # 3. Remove from Redis (release.lua)
    await release_script(...)
```

The release is wrapped in a "super shield" task to ensure it completes even during cancellation.

## Acquisition ID Tracking

The `_AcquisitionIdTracker` maintains a per-task stack of acquisition IDs:

- **Key**: `(task_id, semaphore_name)`
- **Value**: Stack of acquisition IDs (LIFO)

This enables:
- Context manager usage without explicit ID tracking
- Nested acquisitions of the same semaphore
- Cross-task release when acquisition ID is provided explicitly

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

### `get_acquisition_statistics()`

Returns current usage statistics for semaphores:

```python
stats = await RedisSemaphore.get_acquisition_statistics(
    names=["my-sem"],  # or None for all
    limit=100,
    client_manager=my_manager
)
# Returns: {"my-sem": SemaphoreAcquisitionStats(acquired_slots=5, max_slots=10)}
```

Uses Redis `SCAN` to find semaphore keys and pipelines for efficient batch queries.

### `stop_all()`

Gracefully stops all background tasks:

```python
await RedisSemaphore.stop_all()
```

This:
1. Stops all TTL checker tasks
2. Cancels all ping tasks

Call this during application shutdown.

## Thread Safety

- `_PingTasksManager` and `_TTLChecker` registries use `threading.Lock` for thread safety
- Each `RedisClientManager` is bound to a single event loop (validated on use)
- Multiple `RedisSemaphore` instances can share a `RedisClientManager`

## Usage Example

```python
from asyncio_advanced_semaphores.redis import RedisSemaphore, RedisClientManager, RedisConfig

# Configure Redis connection
config = RedisConfig(
    url="redis://localhost:6379",
    namespace="my-app"
)
manager = RedisClientManager(conf=config)

# Create semaphore
sem = RedisSemaphore(
    name="my-resource",
    value=5,
    ttl=300,  # 5 minutes max hold time
    max_acquire_time=60,  # 1 minute max wait
    client_manager=manager
)

# Use as context manager
async with sem:
    # Do work with acquired slot
    pass

# Or manual acquire/release
acquisition = await sem.acquire()
try:
    # Do work
    pass
finally:
    await sem.arelease(acquisition.id)

# Shutdown
await RedisSemaphore.stop_all()
```

