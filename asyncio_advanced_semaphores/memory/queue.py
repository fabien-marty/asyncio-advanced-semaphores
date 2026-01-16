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
class _QueueWithCreationDate:
    """A bounded slot tracker with async waiting semantics.

    Uses a dict internally for O(1) lookup/removal by acquisition_id,
    while maintaining bounded capacity with async waiting when full.
    """

    _items: dict[str, _QueueItem]
    _maxsize: int
    _condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    _creation_perf_counter: float = field(default_factory=time.perf_counter)
    _timers: dict[str, asyncio.TimerHandle] = field(default_factory=dict)

    @classmethod
    def create(cls, maxsize: int) -> Self:
        return cls(_items={}, _maxsize=maxsize)

    def is_empty(self) -> bool:
        return len(self._items) == 0 and len(self._timers) == 0

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def qsize(self) -> int:
        return len(self._items)

    def time_since_creation(self) -> float:
        return time.perf_counter() - self._creation_perf_counter

    def full(self) -> bool:
        return len(self._items) >= self._maxsize

    async def afull(self) -> bool:
        async with self._condition:
            return self.full()

    async def put(self, item: _QueueItem) -> int:
        """Add an item to the tracker, waiting if at capacity.

        Uses a Condition to synchronize with remove operations.

        Returns:
            The number of items in the queue (including the new item).
        """
        async with self._condition:
            while self.full():
                await self._condition.wait()
            self._items[item.acquisition_id] = item
            return len(self._items)

    async def remove(self, acquisition_id: str) -> _QueueItem | None:
        """Remove an item by acquisition_id.

        O(1) removal using dict.pop(). Notifies waiters when space becomes available.
        """
        async with self._condition:
            result = self._items.pop(acquisition_id, None)
            if result is not None:
                self._condition.notify()
            return result

    def add_timer(self, acquisition_id: str, delay: float, callback, *args) -> None:
        """Add a TTL timer for an acquisition.

        Timers are stored at the queue level so any MemorySemaphore instance
        sharing this queue can cancel them on release.
        """
        timer = asyncio.get_running_loop().call_later(delay, callback, *args)
        self._timers[acquisition_id] = timer

    def cancel_and_remove_timer(self, acquisition_id: str) -> None:
        """Cancel and remove a TTL timer for an acquisition.

        Safe to call even if the timer doesn't exist or was already cancelled.
        """
        timer = self._timers.pop(acquisition_id, None)
        if timer is not None:
            timer.cancel()


@dataclass
class _QueueManager:
    empty_queue_max_ttl: float = 60.0
    __lock: threading.Lock = field(default_factory=threading.Lock)
    __queues: dict[str, _QueueWithCreationDate] = field(default_factory=dict)

    def get_or_create_queue(self, name: str, maxsize: int) -> _QueueWithCreationDate:
        with self.__lock:
            if name not in self.__queues:
                # Before creating a new queue, let's cleanup old empty queues
                self._cleanup_old_empty_queues()
                queue = _QueueWithCreationDate.create(maxsize)
                self.__queues[name] = queue
            else:
                queue = self.__queues[name]
                if queue.maxsize != maxsize:
                    raise Exception("Queue maxsize mismatch")
            return queue

    def _cleanup_old_empty_queues(self) -> None:
        queue_names_to_remove: list[str] = []
        for name, queue in self.__queues.items():
            if (
                queue.is_empty()
                and queue.time_since_creation() > self.empty_queue_max_ttl
            ):
                queue_names_to_remove.append(name)
        for name in queue_names_to_remove:
            self.__queues.pop(name)

    @property
    def is_empty(self) -> bool:
        with self.__lock:
            for queue in self.__queues.values():
                if not queue.is_empty():
                    return False
        return True

    def get_size(self) -> int:
        with self.__lock:
            return len(self.__queues)

    def get_acquired_stats(
        self, *, names: list[str] | None = None, limit: int = 100
    ) -> dict[str, SemaphoreStats]:
        with self.__lock:
            results: list[tuple[str, SemaphoreStats]] = []
            for name, queue in self.__queues.items():
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
