# asyncio-advanced-semaphores

**Production-ready asyncio semaphores with TTL, heartbeat, fair-queueing, thread-safety, and distributed support (redis).**

> [!WARNING]
> This project is a work in progress. Do not use it in production environments yet.

## Why Use This?

Traditional `asyncio.Semaphore` works great until it doesn't:

- **Distributed systems with multiple threads/machines?** Good luck coordinating rate limits across multiple instances.
- **TTL support?** If you want to limit the time a task can hold a slot, you need to implement it yourself (and it's not easy!).
- **Heartbeat support?** In distributed mode, you don't want to leak slots for long when a machine is brutally killed.
- **Zero visibility?** No idea which semaphores are congested or why.

This library solves all of that.

## Features

- ⏱️ **TTL (Time To Live)** — Automatic slot expiration prevents deadlocks from crashed tasks
- 💓 **Heartbeat system** — Distributed semaphores stay alive with automatic keep-alive pings
- ⚖️ **Fair queueing** — First-come, first-served acquisition prevents starvation
- 🌐 **Distributed coordination (or not)** — Redis-backed semaphores work across processes and machines, Memory-backed semaphores work only locally
- 🔍 **Built-in observability** — Query acquisition statistics to monitor congestion
- 🛡️ **Thread-safety** — You can acquire semaphores from multiple threads

## Limitations

- We only support an async interface
- We don't support "Redis Cluster" for the moment (for the distributed semaphore implementation)
- All clients must be reasonably time synchronized (NTP)

## Installation

```bash
pip install asyncio-advanced-semaphores
```

## Quickstart

### In-Memory Semaphore

Perfect for single-process rate limiting, connection pooling, or controlling concurrent operations.

```python
import asyncio
from asyncio_advanced_semaphores import MemorySemaphore

# Limit to 10 concurrent operations
sem = MemorySemaphore(name="my-semaphore", value=10)

async def limited_operation():
    # note: cm() means "context manager"
    async with sem.cm() as acquired_result:
        print(f"Acquired slot id={acquired_result.acquisition_id}, slot_number={acquired_result.slot_number}!")
        await asyncio.sleep(1)  # Do some work
    print("Released slot!")

async def main():
    # Run 50 tasks, but only 10 at a time
    await asyncio.gather(*[limited_operation() for _ in range(50)])

asyncio.run(main())
```

### Distributed Semaphore (Redis)

Coordinate rate limits and resource access across multiple services, processes, or machines.

```python
import asyncio
from asyncio_advanced_semaphores import RedisSemaphore, RedisConfig

# Configure Redis connection
config = RedisConfig(
    url="redis://localhost:6379",
    namespace="my-app",
)

# Create a distributed semaphore (same name = shared across all instances)
sem = RedisSemaphore(
    name="external-api-rate-limit",
    value=100,  # Max 100 concurrent API calls across all services
    config=config,
)

async def call_external_api():
    # note: cm() means "context manager"
    async with sem.cm() as acquired_result:
        print(f"Acquired slot id={acquired_result.acquisition_id}, slot_number={acquired_result.slot_number}!")
        await make_api_request()
    print("Released distributed slot!")

asyncio.run(call_external_api())
```

### Manual Acquire/Release

If you need more control, you can manually acquire and release slots using the `acquire()` and `release()` methods:

```python
from asyncio_advanced_semaphores import MemorySemaphore

sem = MemorySemaphore(name="my-semaphore", value=2)

async def manual_usage():
    result = await sem.acquire()
    try:
        # critical section
        print(f"Acquired slot {result.slot_number}")
    finally:
        await sem.release(result.acquisition_id)
```

### Observability

Monitor your semaphores to identify bottlenecks:

```python
from asyncio_advanced_semaphores import MemorySemaphore

# Get statistics for all semaphores
stats = await MemorySemaphore.get_acquired_stats()
for name, stat in stats.items():
    print(f"{name}: {stat.acquired_slots}/{stat.max_slots} ({stat.acquired_percent:.1f}%)")
```

## Advanced Usage

### Different semaphore objects with the same name

You can create different semaphore objects with the same `name` attribute. They will share the same slots.

```python
from asyncio_advanced_semaphores import MemorySemaphore

sem1 = MemorySemaphore(name="my-semaphore", value=1)
sem2 = MemorySemaphore(name="my-semaphore", value=1)

# Let's acquire my-semaphore with the first semaphore object
sem1_acquired_result = await sem1.acquire()

print(await sem2.locked()) # True, because sem1 and sem2 share the same slots (same name)

await sem1.release(sem1_acquired_result.acquisition_id)

print(await sem2.locked()) # False, because sem1 and sem2 share the same slots (same name)
```

### Thread-safety

You can use the semaphore objects from multiple threads. Each thread will have its own event loop. You can share the same semaphore objects across threads or using distinct semaphore objects.

If you use distinct semaphore objects but with the same `name` attribute, they will share the same slots.

### TTL (Time To Live)

You can set the `ttl` attribute to the number of seconds after which the slot will be released automatically.

```python
from asyncio_advanced_semaphores import MemorySemaphore

sem = MemorySemaphore(name="my-semaphore", value=1, ttl=1)

# Let's acquire my-semaphore with the first semaphore object
result = await sem.acquire()

time.sleep(2)
# NOTE: the semaphore will be released automatically after 1 second (TTL)
```

### Max Acquire Time

You can set the `max_acquire_time` attribute to the maximum number of seconds to wait for the slot to be acquired.
If the slot is not acquired within the timeout, an `TimeoutError` is raised.

```python
from asyncio_advanced_semaphores import MemorySemaphore

sem = MemorySemaphore(name="my-semaphore", value=1, max_acquire_time=1)

# Let's acquire my-semaphore with the first semaphore object
result1 = await sem.acquire()

try:
    result2 = await sem.acquire()
except TimeoutError:
    # TimeoutError will be raised after 1 second (max_acquire_time)
    pass

await sem.release(result1.acquisition_id)
```

### Heartbeat

With `RedisSemaphore`, if you set a long `ttl` value (or no TTL at all), you can set a `heartbeat_max_interval` (default to 180 seconds) value to keep the slot alive.

```python
from asyncio_advanced_semaphores import RedisSemaphore, RedisConfig

# Configure Redis connection
config = RedisConfig(
    url="redis://localhost:6379",
    namespace="my-app",
)

sem = RedisSemaphore(name="my-semaphore", value=1, ttl=86400, heartbeat_max_interval=180, config=config)

# Let's acquire the semaphore
result = await sem.acquire()

# do your async work...
# (a heartbeat will be automatically and regularly sent to the Redis server to keep the slot alive)

# If the program or the machine is brutally killed, the semaphore won't be released (no time to do that!)
# But the heartbeat task will disappear also and the semaphore will be released automatically after the
# `heartbeat_max_interval` value (180 seconds) which is a lot lower than the `ttl` value (86400 seconds).
```

[!WARNING]
The heartbeat task is completly automatic (you don't have to do anything to keep the slot alive) but this is an
asynchronous task. So don't block the event loop for too long to avoid blocking the heartbeat task and automatically
releasing the slot.

## DEV

This library is managed by [UV](https://docs.astral.sh/uv/) and a `Makefile`.

`make help` to see the available commands.

Some architecture notes are available:
- [MemorySemaphore](asyncio_advanced_semaphores/memory/README.md)
- [Redis Lua Scripts](asyncio_advanced_semaphores/redis/lua/README.md)
- [RedisSemaphore](asyncio_advanced_semaphores/redis/README.md)

## License

MIT
