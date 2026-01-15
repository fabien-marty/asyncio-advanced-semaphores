# asyncio-advanced-semaphores

**Production-ready asyncio semaphores with TTL, fair queueing, and distributed support.**

## Why Use This?

Traditional `asyncio.Semaphore` works great until it doesn't:

- **Distributed systems with multiple machines?** Good luck coordinating rate limits across multiple instances.
- **TTL support?** If you want to limit the time a task can hold a slot, you need to implement it yourself (and it's not easy!).
- **Zero visibility?** No idea which semaphores are congested or why.

This library solves all of that with a familiar, drop-in API.

## Features

- 🔄 **Drop-in compatible** with `asyncio.Semaphore` interface
- ⏱️ **TTL (Time To Live)** — Automatic slot expiration prevents deadlocks from crashed tasks
- 💓 **Heartbeat system** — Distributed semaphores stay alive with automatic keep-alive pings
- ⚖️ **Fair queueing** — First-come, first-served acquisition prevents starvation
- 🌐 **Distributed coordination** — Redis-backed semaphores work across processes and machines
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
from asyncio_advanced_semaphores import RedisSemaphore, RedisClientManager
from asyncio_advanced_semaphores.redis.conf import RedisConfig

# Configure Redis connection
config = RedisConfig(
    url="redis://localhost:6379",
    namespace="my-app",
)
client_manager = RedisClientManager(conf=config)

# Create a distributed semaphore (same name = shared across all instances)
sem = RedisSemaphore(
    name="external-api-rate-limit",
    value=100,  # Max 100 concurrent API calls across all services
    client_manager=client_manager,
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
stats = await MemorySemaphore.get_acquisition_statistics()
for name, stat in stats.items():
    print(f"{name}: {stat.acquired_slots}/{stat.max_slots} ({stat.acquired_percent:.1f}%)")
```

## License

MIT
