import asyncio
import uuid
from dataclasses import dataclass, field

from asyncio_advanced_semaphores.common import (
    AcquireResult,
    Semaphore,
    SemaphoreStats,
)
from asyncio_advanced_semaphores.memory.queue import (
    _DEFAULT_QUEUE_MANAGER,
    _BoundedQueue,
    _QueueItem,
    _QueueManager,
)


def _get_current_task() -> asyncio.Task:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task(loop)
    if task is None:
        raise Exception("No running asyncio task found")  # pragma: no cover
    return task


@dataclass(kw_only=True)
class MemorySemaphore(Semaphore):
    cancel_task_after_ttl: bool = False
    """If set to True, the task that acquired the slot is cancelled automatically if it's not released within the TTL.
    
    """
    _queue_manager: _QueueManager = field(
        default_factory=lambda: _DEFAULT_QUEUE_MANAGER
    )

    def _get_type(self) -> str:
        return "in-memory"

    @property
    def _queue(self) -> _BoundedQueue:
        """Get the queue from the manager.

        This is a property (not cached) to ensure we always get the canonical
        queue from the manager, even after queue cleanup.
        """
        return self._queue_manager.get_or_create_queue(self.name, self.value)

    async def locked(self) -> bool:
        return self._queue.full()

    async def _acquire(self) -> AcquireResult:
        acquisition_id = uuid.uuid4().hex
        task = _get_current_task()
        try:
            async with asyncio.timeout(self.max_acquire_time):
                slot_number = await self._queue.put(
                    _QueueItem(task=task, acquisition_id=acquisition_id)
                )
        except (asyncio.CancelledError, TimeoutError):
            # Make sure to not leak a slot of the semaphore on cancellation or timeout.
            # TimeoutError can be raised by asyncio.timeout's __aexit__ if the timeout
            # fires just as put() completes (narrow race window).
            self.__release(acquisition_id)
            raise
        if self.ttl is not None:
            self._queue.add_timer(
                acquisition_id, self.ttl, self._schedule_expire, acquisition_id
            )
        return AcquireResult(acquisition_id=acquisition_id, slot_number=slot_number)

    def __release(self, acquisition_id: str) -> _QueueItem | None:
        self._queue.cancel_and_remove_timer(acquisition_id)
        return self._queue.remove(acquisition_id)

    def _schedule_expire(self, acquisition_id: str) -> None:
        """Handle expiration (called from timer callback)."""
        self._expire(acquisition_id)

    def _expire(self, acquisition_id: str) -> None:
        """Handle TTL expiration."""
        self._logger.warning("TTL expired => let's release a slot of the semaphore")
        item = self.__release(acquisition_id)
        if item is not None:
            if self.cancel_task_after_ttl:
                item.task.cancel()

    async def _release(self, acquisition_id: str) -> None:
        self.__release(acquisition_id)

    @classmethod
    async def get_acquired_stats(
        cls,
        *,
        names: list[str] | None = None,
        limit: int = 100,
        queue_manager: _QueueManager = _DEFAULT_QUEUE_MANAGER,
    ) -> dict[str, SemaphoreStats]:
        return queue_manager.get_acquired_stats(names=names, limit=limit)
