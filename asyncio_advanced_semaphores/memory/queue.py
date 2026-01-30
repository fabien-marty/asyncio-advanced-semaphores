import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Self

from asyncio_advanced_semaphores.common import SemaphoreStats


@dataclass
class _QueueItem:
    task: asyncio.Task
    acquisition_id: str


@dataclass
class _BoundedQueue:
    """A bounded slot tracker with async waiting semantics (like a queue).

    Uses a dict internally for O(1) lookup/removal by acquisition_id,
    while maintaining bounded capacity with async waiting when full.

    This class is thread-safe. It uses a threading.Lock to protect shared
    state and loop.call_soon_threadsafe() for cross-thread notifications.
    """

    _items: dict[str, _QueueItem]
    """Items in the queue.
    
    acquisition_id -> _QueueItem
    """

    _maxsize: int
    """Maximum size of the queue."""

    _lock: threading.Lock = field(default_factory=threading.Lock)
    """Lock to protect shared state."""

    _waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = field(
        default_factory=list
    )
    """Waiters for when the queue is full."""

    _last_touch_perf_counter: float = field(default_factory=time.perf_counter)
    """Last touch timestamp (updated when queue reference is obtained)."""

    _timers: dict[str, tuple[asyncio.AbstractEventLoop, asyncio.TimerHandle]] = field(
        default_factory=dict
    )
    """Timers for TTL expiration.
    
    acquisition_id -> (event_loop, TimerHandle)

    """

    @classmethod
    def create(cls, maxsize: int) -> Self:
        return cls(_items={}, _maxsize=maxsize)

    def _is_empty_unlocked(self) -> bool:
        """Check if queue is empty. Must be called with _lock held."""
        return (
            len(self._items) == 0 and len(self._timers) == 0 and len(self._waiters) == 0
        )

    def is_empty(self) -> bool:
        with self._lock:
            return self._is_empty_unlocked()

    def is_stale_for_cleanup(self, max_ttl: float) -> bool:
        """Check if queue is empty and old enough to be cleaned up.

        This method atomically checks both conditions while holding the lock,
        preventing races where a waiter could be added between checks.
        """
        with self._lock:
            return (
                self._is_empty_unlocked()
                and self._time_since_last_usage_unlocked() > max_ttl
            )

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def qsize(self) -> int:
        with self._lock:
            return len(self._items)

    def touch(self) -> None:
        """Update last usage timestamp to prevent premature cleanup."""
        with self._lock:
            self._last_touch_perf_counter = time.perf_counter()

    def _time_since_last_usage_unlocked(self) -> float:
        """Return time since last usage. Must be called with _lock held."""
        return time.perf_counter() - self._last_touch_perf_counter

    def _full_unlocked(self) -> bool:
        return len(self._items) >= self._maxsize

    def full(self) -> bool:
        with self._lock:
            return self._full_unlocked()

    async def put(self, item: _QueueItem) -> int:
        """Add an item to the tracker, waiting if at capacity.

        Thread-safe implementation using threading.Lock and asyncio.Event
        for cross-thread notifications.

        Returns:
            The number of items in the queue (including the new item).
        """
        loop = asyncio.get_running_loop()

        while True:
            with self._lock:
                if not self._full_unlocked():
                    self._items[item.acquisition_id] = item
                    return len(self._items)
                # Queue is full, need to wait
                event = asyncio.Event()
                waiter = (loop, event)
                self._waiters.append(waiter)

            try:
                # Wait outside the lock to avoid blocking other threads
                await event.wait()
            except asyncio.CancelledError:
                # Clean up our waiter entry to prevent lost wakeups
                with self._lock:
                    try:
                        self._waiters.remove(waiter)
                    except ValueError:
                        # Our waiter was already popped by _notify_one_waiter_unlocked,
                        # meaning we "received" a notification but can't use it since
                        # we're being cancelled. Forward the notification to the next
                        # waiter to prevent lost wakeups.
                        self._notify_one_waiter_unlocked()
                raise

    def remove(self, acquisition_id: str) -> _QueueItem | None:
        """Remove an item by acquisition_id.

        O(1) removal using dict.pop(). Notifies waiters when space becomes available.
        """
        with self._lock:
            result = self._items.pop(acquisition_id, None)
            if result is not None:
                self._notify_one_waiter_unlocked()
            return result

    def _notify_one_waiter_unlocked(self) -> None:
        """Notify one waiter that space might be available.

        Must be called with _lock held.
        """
        while self._waiters:
            loop, event = self._waiters.pop(0)
            try:
                loop.call_soon_threadsafe(event.set)
                return
            except RuntimeError:
                # Loop might be closed, try next waiter
                continue

    def add_timer(self, acquisition_id: str, delay: float, callback, *args) -> None:
        """Add a TTL timer for an acquisition.

        Timers are stored at the queue level so any MemorySemaphore instance
        sharing this queue can cancel them on release.
        """
        loop = asyncio.get_running_loop()
        with self._lock:
            timer = loop.call_later(delay, callback, *args)
            self._timers[acquisition_id] = (loop, timer)

    def cancel_and_remove_timer(self, acquisition_id: str) -> None:
        """Cancel and remove a TTL timer for an acquisition.

        Thread-safe: uses call_soon_threadsafe to cancel the timer on its
        original event loop, since TimerHandle is not officially thread-safe.

        Safe to call even if the timer doesn't exist or was already cancelled.
        """
        with self._lock:
            timer_tuple = self._timers.pop(acquisition_id, None)
        if timer_tuple is not None:
            loop, timer = timer_tuple
            try:
                loop.call_soon_threadsafe(timer.cancel)
            except RuntimeError:
                # Loop might be closed, timer will be cleaned up anyway
                pass


@dataclass
class _QueueManager:
    """Manager for _QueueWithCreationDate instances.

    Thread-safe.
    """

    empty_queue_max_ttl: float = 60.0
    """Maximum time to keep an empty queue before cleaning it up."""

    _lock: threading.Lock = field(default_factory=threading.Lock)
    """Lock to protect shared state."""

    _queues: dict[str, _BoundedQueue] = field(default_factory=dict)
    """Queues by name.
    
    name -> _QueueWithCreationDate
    """

    def get_or_create_queue(self, name: str, maxsize: int) -> _BoundedQueue:
        def create_queue(maxsize: int) -> _BoundedQueue:
            # Before creating a new queue, let's cleanup old empty queues
            return _BoundedQueue.create(maxsize)

        with self._lock:
            if name not in self._queues:
                # Before creating a new queue, let's cleanup old empty queues
                self._cleanup_old_empty_queues()
                queue = create_queue(maxsize)
                self._queues[name] = queue
            else:
                queue = self._queues[name]
                if queue.maxsize != maxsize:
                    if queue.is_empty():
                        # The queue is empty so we can rebuilt it to change the maxsize
                        queue = create_queue(maxsize)
                        self._queues[name] = queue
                    else:
                        raise Exception(
                            "There is already a used semaphore with this name but the maxsize mismatch => fix the maxsize or release all slots of the old semaphore"
                        )
            # Touch the queue to prevent cleanup while reference is held
            queue.touch()
            return queue

    def _cleanup_old_empty_queues(self) -> None:
        queue_names_to_remove: list[str] = []
        for name, queue in self._queues.items():
            if queue.is_stale_for_cleanup(self.empty_queue_max_ttl):
                queue_names_to_remove.append(name)
        for name in queue_names_to_remove:
            self._queues.pop(name)

    @property
    def is_empty(self) -> bool:
        with self._lock:
            for queue in self._queues.values():
                if not queue.is_empty():
                    return False
        return True

    def get_size(self) -> int:
        with self._lock:
            return len(self._queues)

    def get_acquired_stats(
        self, *, names: list[str] | None = None, limit: int = 100
    ) -> dict[str, SemaphoreStats]:
        with self._lock:
            results: list[tuple[str, SemaphoreStats]] = []
            for name, queue in self._queues.items():
                if names is not None and name not in names:
                    continue
                queue_size = queue.qsize()
                queue_max_size = queue.maxsize
                assert queue_max_size > 0
                if queue_size == 0 and names is None:
                    continue
                results.append(
                    (
                        name,
                        SemaphoreStats(
                            acquired_slots=queue_size, max_slots=queue_max_size
                        ),
                    )
                )
            sorted_results = sorted(
                results, key=lambda x: x[1].acquired_percent, reverse=True
            )
            return {name: stats for name, stats in sorted_results[:limit]}


_DEFAULT_QUEUE_MANAGER = _QueueManager()
