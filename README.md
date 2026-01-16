# asyncio-advanced-semaphores

**Production-ready asyncio semaphores with TTL, heartbeat, fair-queueing, and distributed support (redis).**

## Why Use This?

Traditional `asyncio.Semaphore` works great until it doesn't:

- **Distributed systems with multiple machines?** Good luck coordinating rate limits across multiple instances.
- **TTL support?** If you want to limit the time a task can hold a slot, you need to implement it yourself (and it's not easy!).
- **Heartbeat support?** In distributed mode, you don't want to leak slots for long when a machine is brutally killed.
- **Zero visibility?** No idea which semaphores are congested or why.

This library solves all of that with a familiar, drop-in API.

## Features

- 🔄 **Drop-in compatible** with `asyncio.Semaphore` interface
- ⏱️ **TTL (Time To Live)** — Automatic slot expiration prevents deadlocks from crashed tasks
- 💓 **Heartbeat system** — Distributed semaphores stay alive with automatic keep-alive pings
- ⚖️ **Fair queueing** — First-come, first-served acquisition prevents starvation
- 🌐 **Distributed coordination (or not)** — Redis-backed semaphores work across processes and machines, Memory-backed semaphores work only locally
- 🔍 **Built-in observability** — Query acquisition statistics to monitor congestion
- 🛡️ **Cancellation-safe** — Proper cleanup on task cancellation, no leaked slots

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
sem = MemorySemaphore(value=10)

async def limited_operation():
    async with sem:
        print("Acquired slot!")
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
    async with sem:
        print("Acquired distributed slot!")
        await make_api_request()
    print("Released distributed slot!")

asyncio.run(call_external_api())
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

## Important note about the implementation

This library provides a **drop-in replacement** for `asyncio.Semaphore` — the `acquire()`, `release()`, and async context manager interfaces work exactly as expected.

However, some advanced features require a specific usage pattern when **reusing the same semaphore object** for multiple simultaneous acquisitions (i.e., calling `acquire()` multiple times on the same object before releasing).

### When is this relevant?

This limitation applies **only when `value > 1`** and you want to use:

| Semaphore Type | Feature Restriction |
|----------------|---------------------|
| `MemorySemaphore` | `ttl` + `cancel_task_after_ttl=True` |
| `RedisSemaphore` | `ttl` or `heartbeat_max_interval` (enabled by default) |

### What happens if you violate this?

If you try to call `acquire()` on the same semaphore object while it already holds an acquisition, you'll get an explicit exception:

```
Exception: This semaphore doesn't support multiple simultaneous acquisitions
(with selected settings) => change the settings or use multiple semaphore
objects with the same name.
```

### The solution: use separate objects with the same name

Instead of reusing the same semaphore object, create multiple semaphore objects with the **same name**. They will share the same underlying slots:

```python
import asyncio
from asyncio_advanced_semaphores import RedisSemaphore, RedisConfig

config = RedisConfig(url="redis://localhost:6379")

async def task_a():
    # Each task creates its own semaphore object with the same name
    sem = RedisSemaphore(name="shared-resource", value=5, config=config)
    async with sem:
        await do_work()

async def task_b():
    # Same name = same shared slots, but separate object = no conflict
    sem = RedisSemaphore(name="shared-resource", value=5, config=config)
    async with sem:
        await do_other_work()

async def main():
    await asyncio.gather(task_a(), task_b())
```

## DEV

This library is managed by [UV](https://docs.astral.sh/uv/) and a `Makefile`.

`make help` to see the available commands.

Some architecture notes are available:
- [MemorySemaphore](asyncio_advanced_semaphores/memory/README.md)
- [Redis Lua Scripts](asyncio_advanced_semaphores/redis/lua/README.md)
- [RedisSemaphore](asyncio_advanced_semaphores/redis/README.md)

## License

MIT
