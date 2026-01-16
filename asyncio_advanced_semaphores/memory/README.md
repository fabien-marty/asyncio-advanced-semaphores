# MemorySemaphore - Internal Documentation

This document explains how `MemorySemaphore` works internally. It's an in-memory implementation of an advanced asyncio semaphore with features beyond the standard `asyncio.Semaphore`.

## Features

- **Drop-in compatible**: Same interface as `asyncio.Semaphore` (`acquire()` returns `bool`, `release()` takes no arguments)
- **Named semaphores**: Multiple instances with the same name share the same underlying slots
- **Time-to-live (TTL)**: Acquired slots are automatically released after a configurable duration
- **Acquire timeout**: Maximum time to wait when acquiring a slot (`max_acquire_time`)
- **Task auto-cancellation**: Optionally cancel tasks when their TTL expires (`cancel_task_after_ttl`)
- **Acquisition statistics**: Monitor semaphore usage across the application
- **Thread-safe**: Queue management operations are protected by locks

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

### Synchronization with asyncio.Condition

The slot tracker (`_QueueWithCreationDate`) uses an `asyncio.Condition` to synchronize access between `put()` and `remove()` operations:

- **`put()`**: Acquires the condition lock, waits if dict is full, then adds the item atomically. Returns the number of items in the queue (including the new item), used as the slot number.
- **`remove()`**: Acquires the condition lock, removes the item by key in O(1), then notifies waiters

This ensures that no `put()` can sneak in while a `remove()` is in progress.

## Components

### _QueueManager

Located in `queue.py`, the `_QueueManager` is responsible for:

- **Creating and caching slot trackers**: Maps semaphore names to their underlying trackers
- **Tracker sharing**: Semaphores with the same name share the same tracker
- **Automatic cleanup**: Removes old empty trackers after `empty_queue_max_ttl` (default: 60 seconds)
- **Statistics**: Provides acquisition statistics across all managed semaphores
- **Thread safety**: Uses a `threading.Lock` to protect tracker creation and access

A default global `_QueueManager` instance (`_DEFAULT_QUEUE_MANAGER`) is used unless a custom one is provided.

### _QueueWithCreationDate

A bounded slot tracker using a dict internally that provides:

- **O(1) operations**: Uses a dict for constant-time lookup and removal by acquisition ID
- **Synchronized access**: Uses an `asyncio.Condition` to coordinate `put()` and `remove()` operations
- **Bounded capacity**: Blocks on `put()` when at max capacity, notifies waiters on `remove()`
- **Creation timestamp**: Tracks when the tracker was created for cleanup purposes

### _QueueItem

A simple dataclass stored in the tracker on each acquisition:

```python
@dataclass
class _QueueItem:
    task: asyncio.Task      # The task that acquired the slot
    acquisition_id: str     # Unique identifier for this acquisition
```

### Acquisition ID Stack

Each `Semaphore` instance maintains an internal `__acquisition_id_stack` (a list) that tracks acquisition IDs:

- Uses LIFO (stack) to support **nested acquisitions** of the same semaphore
- When `release()` is called, it pops the most recent acquisition ID from the stack
- This is managed internally by the base `Semaphore` class, making the API compatible with `asyncio.Semaphore`

## Acquisition Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        acquire()                                │
├─────────────────────────────────────────────────────────────────┤
│  1. Generate unique acquisition_id (UUID)                       │
│                              │                                  │
│                              ▼                                  │
│  2. await tracker.put(_QueueItem(task, acquisition_id))         │
│     └─► Acquires Condition lock                                 │
│     └─► If dict full: WAIT on Condition (with max_acquire_time) │
│     └─► Put item and release lock                               │
│     └─► Returns slot_number (count of items in queue)           │
│                              │                                  │
│                              ▼                                  │
│  3. If TTL set: schedule timer to call _expire() after TTL      │
│                              │                                  │
│                              ▼                                  │
│  4. Push acquisition_id to internal stack                       │
│                              │                                  │
│                              ▼                                  │
│  5. Log acquisition (name, acquire_time, slot_number, max_slots)│
│                              │                                  │
│                              ▼                                  │
│  6. Return True (compatible with asyncio.Semaphore)             │
└─────────────────────────────────────────────────────────────────┘
```

## Release Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         arelease()                              │
├─────────────────────────────────────────────────────────────────┤
│  1. Pop acquisition_id from internal stack                      │
│     └─► Gets most recent ID (LIFO for nested acquisitions)      │
│                              │                                  │
│                              ▼                                  │
│  2. Cancel any pending TTL timer for this acquisition_id        │
│                              │                                  │
│                              ▼                                  │
│  3. await tracker.remove(acquisition_id)                        │
│     └─► Acquires Condition lock                                 │
│     └─► Removes item by key in O(1)                             │
│     └─► Notifies waiting acquirers that space is available      │
│     └─► Releases lock                                           │
│                              │                                  │
│                              ▼                                  │
│  4. Log release (name, type)                                    │
└─────────────────────────────────────────────────────────────────┘

Note: The sync release() method schedules the release as a background
task and returns immediately, matching asyncio.Semaphore.release() behavior.
```

## TTL Expiration Flow

When TTL expires for an acquisition:

```
┌─────────────────────────────────────────────────────────────────┐
│              Timer callback → _schedule_expire()                │
├─────────────────────────────────────────────────────────────────┤
│  1. Create background task for async _expire()                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   async _expire(acquisition_id)                 │
├─────────────────────────────────────────────────────────────────┤
│  1. Log warning about TTL expiration                            │
│                              │                                  │
│                              ▼                                  │
│  2. await _release() to remove from slot tracker (synchronized) │
│                              │                                  │
│                              ▼                                  │
│  3. If cancel_task_after_ttl=True:                              │
│     └─► Cancel the task that was holding the slot               │
└─────────────────────────────────────────────────────────────────┘
```

This is useful for preventing deadlocks when tasks hang or take too long.
The expiration handler runs as a background task to allow proper async
synchronization with the tracker's Condition lock.

## Usage Examples

### Basic usage with context manager

```python
sem = MemorySemaphore(value=5, name="my-resource")

async with sem:
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

async with sem:
    await potentially_hanging_operation()
```

### Shared semaphores across instances

```python
# These two instances share the same underlying queue
sem1 = MemorySemaphore(value=3, name="shared")
sem2 = MemorySemaphore(value=3, name="shared")

async with sem1:  # Uses 1 of 3 slots
    async with sem2:  # Uses another slot from the SAME semaphore
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
    async with sem:
        await do_work()
except asyncio.TimeoutError:
    print("Could not acquire semaphore in time")
```

### Monitoring acquisition statistics

```python
stats = await MemorySemaphore.get_acquired_stats()
for name, stat in stats.items():
    print(f"{name}: {stat.acquired_slots}/{stat.max_slots} ({stat.acquired_percent:.1f}%)")
```

## Thread Safety and Synchronization

- The `_QueueManager` uses a `threading.Lock` to protect tracker creation and access
- The `_QueueWithCreationDate` uses an `asyncio.Condition` to synchronize `put()` and `remove()` operations
- TTL timers use `asyncio.TimerHandle` which are event loop-safe
- Background tasks (for TTL expiration and sync release) are tracked to prevent garbage collection

## Memory Management

The `_QueueManager` automatically cleans up old empty trackers to prevent memory leaks:

- Trackers that have been empty for longer than `empty_queue_max_ttl` (default: 60s) are removed
- Cleanup runs lazily when a new tracker is created
- You can also call `_cleanup_old_empty_queues()` manually if needed

### Tracker Reference Handling

Semaphore instances do **not** cache their tracker reference. Instead, they fetch the tracker from the `_QueueManager` on each operation via a `@property` (not `@cached_property`). This ensures that:

- After tracker cleanup, semaphores automatically get the new canonical tracker
- Multiple semaphore instances with the same name always share the same tracker
- No orphaned tracker references can occur after cleanup

