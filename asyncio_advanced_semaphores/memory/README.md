# MemorySemaphore - Internal Documentation

This document explains how `MemorySemaphore` works internally. It's an in-memory implementation of an advanced asyncio semaphore with features beyond the standard `asyncio.Semaphore`.

## Features

- **Named semaphores**: Multiple instances with the same name share the same underlying slots
- **Time-to-live (TTL)**: Acquired slots are automatically released after a configurable duration
- **Acquire timeout**: Maximum time to wait when acquiring a slot (`max_acquire_time`)
- **Task auto-cancellation**: Optionally cancel tasks when their TTL expires (`cancel_task_after_ttl`)
- **Acquisition statistics**: Monitor semaphore usage across the application
- **Thread-safe**: All operations are protected by `threading.Lock` with cross-thread asyncio notifications

## Core Architecture

### Slot tracker design

The key insight is that `MemorySemaphore` uses a bounded dict (with a fixed `maxsize` equal to the semaphore value) instead of a traditional counter. This inverts the classic semaphore model:

| Traditional Semaphore | MemorySemaphore |
|-----------------------|-----------------|
| Counter starts at N | Dict with maxsize N (starts empty) |
| Decrement on acquire | Put item into dict on acquire |
| Increment on release | Remove item from dict on release |
| Block when counter = 0 | Block when dict is full |

When the dict is full (all slots acquired), new `put()` calls will block until space becomes available (i.e., until someone releases a slot).

### Why a dict?

Using a dict provides several advantages:

1. **Tracking acquisitions**: Each slot holds a `_QueueItem` containing the task and acquisition ID
2. **O(1) removal**: Items can be removed by acquisition ID in constant time
3. **Named semaphores**: Multiple semaphore instances can share the same underlying slot tracker
4. **TTL support**: We know which task holds each slot, enabling targeted release
5. **Task cancellation**: We can cancel the specific task holding an expired slot

### Thread-safe synchronization with threading.Lock and asyncio.Event

The slot tracker (`_BoundedQueue`) uses a `threading.Lock` combined with `asyncio.Event` for cross-thread synchronization:

- **`put()`**: Acquires the lock, checks if dict is full. If full, creates an `asyncio.Event`, adds it to the waiters list (along with the current event loop), releases the lock, then awaits the event. When notified, re-acquires the lock and retries. Returns the number of items in the queue (including the new item), used as the slot number.
- **`remove()`**: Synchronous operation. Acquires the lock, removes the item by key in O(1), notifies one waiter using `loop.call_soon_threadsafe()`, then releases the lock.

This design ensures true thread-safety: the `threading.Lock` protects shared state, while `loop.call_soon_threadsafe()` safely notifies waiters across different threads/event loops.

## Components

### _QueueManager

Located in `queue.py`, the `_QueueManager` is responsible for:

- **Creating and caching slot trackers**: Maps semaphore names to their underlying trackers
- **Tracker sharing**: Semaphores with the same name share the same tracker
- **Automatic cleanup**: Removes stale empty trackers after `empty_queue_max_ttl` (default: 60 seconds)
- **Statistics**: Provides acquisition statistics across all managed semaphores
- **Thread safety**: Uses a `threading.Lock` to protect tracker creation and access

A default global `_QueueManager` instance (`_DEFAULT_QUEUE_MANAGER`) is used unless a custom one is provided.

### _BoundedQueue

A bounded slot tracker using a dict internally that provides:

- **O(1) operations**: Uses a dict for constant-time lookup and removal by acquisition ID
- **Thread-safe synchronized access**: Uses a `threading.Lock` to protect shared state and `asyncio.Event` with `loop.call_soon_threadsafe()` for cross-thread waiter notifications
- **Bounded capacity**: Blocks on `put()` when at max capacity, notifies waiters on `remove()`
- **Touch timestamp**: Tracks when the tracker was last used via `touch()` for cleanup purposes
- **TTL timer management**: Stores and manages TTL timers for acquisitions, allowing any semaphore instance sharing the queue to cancel them

### _QueueItem

A simple dataclass stored in the tracker on each acquisition:

```python
@dataclass
class _QueueItem:
    task: asyncio.Task      # The task that acquired the slot
    acquisition_id: str     # Unique identifier for this acquisition
```

### Acquisition IDs

Each acquisition is tracked with a unique `acquisition_id`:

- Generated as a UUID when `acquire()` is called
- Returned to the caller as part of the `AcquireResult` object
- Must be passed to `release()` to release the specific slot
- Enables precise tracking and release of individual acquisitions

## Acquisition Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        acquire()                                │
├─────────────────────────────────────────────────────────────────┤
│  1. Generate unique acquisition_id (UUID)                       │
│                              │                                  │
│                              ▼                                  │
│  2. await tracker.put(_QueueItem(task, acquisition_id))         │
│     └─► Acquires threading.Lock                                 │
│     └─► If dict full: create Event, add to waiters, wait        │
│     └─► Put item and release lock                               │
│     └─► Returns slot_number (count of items in queue)           │
│                              │                                  │
│                              ▼                                  │
│  3. If TTL set: add timer to tracker via add_timer()            │
│                              │                                  │
│                              ▼                                  │
│  4. Log acquisition (name, acquire_time, slot_number, max_slots)│
│                              │                                  │
│                              ▼                                  │
│  5. Return AcquireResult(acquisition_id, slot_number)           │
└─────────────────────────────────────────────────────────────────┘
```

## Release Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                  release(acquisition_id)                        │
├─────────────────────────────────────────────────────────────────┤
│  1. Cancel any pending TTL timer via cancel_and_remove_timer()  │
│                              │                                  │
│                              ▼                                  │
│  2. tracker.remove(acquisition_id)  [synchronous]               │
│     └─► Acquires threading.Lock                                 │
│     └─► Removes item by key in O(1)                             │
│     └─► Notifies one waiter via loop.call_soon_threadsafe()     │
│     └─► Releases lock                                           │
│                              │                                  │
│                              ▼                                  │
│  3. Log release (name, type)                                    │
└─────────────────────────────────────────────────────────────────┘
```

## TTL Expiration Flow

When TTL expires for an acquisition:

```
┌─────────────────────────────────────────────────────────────────┐
│         Timer callback → _schedule_expire() → _expire()        │
├─────────────────────────────────────────────────────────────────┤
│  1. Log warning about TTL expiration                            │
│                              │                                  │
│                              ▼                                  │
│  2. __release() to remove from slot tracker (synchronous)       │
│     └─► cancel_and_remove_timer() for the acquisition           │
│     └─► tracker.remove() with threading.Lock                    │
│                              │                                  │
│                              ▼                                  │
│  3. If cancel_task_after_ttl=True:                              │
│     └─► Cancel the task that was holding the slot               │
└─────────────────────────────────────────────────────────────────┘
```

This is useful for preventing deadlocks when tasks hang or take too long.
The expiration handler is synchronous since `remove()` is now a synchronous
operation protected by `threading.Lock`.

## Usage Examples

### Basic usage with context manager

```python
sem = MemorySemaphore(value=5, name="my-resource")

async with sem.cm():
    # At most 5 concurrent executions reach here
    await do_work()
# Slot automatically released on exit
```

### With TTL and auto-cancel

```python
sem = MemorySemaphore(
    value=1,
    name="critical-resource",
    ttl=30,                    # Auto-release after 30 seconds
    cancel_task_after_ttl=True # Also cancel the hung task
)

async with sem.cm():
    await potentially_hanging_operation()
```

### Shared semaphores across instances

```python
# These two instances share the same underlying queue
sem1 = MemorySemaphore(value=3, name="shared")
sem2 = MemorySemaphore(value=3, name="shared")

async with sem1.cm():  # Uses 1 of 3 slots
    async with sem2.cm():  # Uses another slot from the SAME semaphore
        # Only 1 slot remaining for "shared"
        pass
```

### With acquire timeout

```python
sem = MemorySemaphore(
    value=1,
    name="limited-resource",
    max_acquire_time=5.0  # Raise TimeoutError if can't acquire within 5s
)

try:
    async with sem.cm():
        await do_work()
except TimeoutError:
    print("Could not acquire semaphore in time")
```

### Manual acquire/release

```python
sem = MemorySemaphore(value=2, name="my-resource")

result = await sem.acquire()
try:
    # critical section
    print(f"Acquired slot {result.slot_number}")
finally:
    await sem.release(result.acquisition_id)
```

### Monitoring acquisition statistics

```python
stats = await MemorySemaphore.get_acquired_stats()
for name, stat in stats.items():
    print(f"{name}: {stat.acquired_slots}/{stat.max_slots} ({stat.acquired_percent:.1f}%)")
```

## Thread Safety and Synchronization

- The `_QueueManager` uses a `threading.Lock` to protect tracker creation and access
- The `_BoundedQueue` uses a `threading.Lock` to protect shared state
- Cross-thread waiter notification uses `loop.call_soon_threadsafe()` with `asyncio.Event`
- Waiters are tracked as tuples of `(event_loop, asyncio.Event)` to ensure notifications reach the correct event loop
- TTL timers are stored at the queue level and managed via `add_timer()` and `cancel_and_remove_timer()` methods
- The `remove()` operation is synchronous, avoiding the need for async coordination during release

## Memory Management

The `_QueueManager` automatically cleans up stale empty trackers to prevent memory leaks:

- Trackers that have been idle (no usage) for longer than `empty_queue_max_ttl` (default: 60s) are removed
- Cleanup runs lazily when a new tracker is created
- A tracker is considered empty only when it has no items, no pending TTL timers, AND no waiters
- The `touch()` method is called when a queue reference is obtained, updating its last usage timestamp
- You can check staleness via `is_stale_for_cleanup(max_ttl)` which atomically checks emptiness and idle time

### Tracker Reference Handling

Semaphore instances do **not** cache their tracker reference. Instead, they fetch the tracker from the `_QueueManager` on each operation via a `@property` (not `@cached_property`). This ensures that:

- After tracker cleanup, semaphores automatically get the new canonical tracker
- Multiple semaphore instances with the same name always share the same tracker
- No orphaned tracker references can occur after cleanup
