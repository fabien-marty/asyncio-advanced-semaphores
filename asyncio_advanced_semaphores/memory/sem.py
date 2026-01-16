import asyncio
import uuid
from dataclasses import dataclass, field

from asyncio_advanced_semaphores.common import (
    Semaphore,
    SemaphoreStats,
    _AcquireResult,
)
from asyncio_advanced_semaphores.memory.queue import (
    _DEFAULT_QUEUE_MANAGER,
    _QueueItem,
    _QueueManager,
    _QueueWithCreationDate,
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
    __background_tasks: set[asyncio.Task] = field(default_factory=set)

    def _get_type(self) -> str:
        return "in-memory"

    def supports_multiple_simultaneous_acquisitions(self) -> bool:
        if self.value == 1 or self.ttl is None or not self.cancel_task_after_ttl:
            return True
        return False

    @property
    def _queue(self) -> _QueueWithCreationDate:
        """Get the queue from the manager.

        This is a property (not cached) to ensure we always get the canonical
        queue from the manager, even after queue cleanup.
        """
        return self._queue_manager.get_or_create_queue(self.name, self.value)

    def locked(self) -> bool:
        return self._queue.full()

    async def alocked(self) -> bool:
        return await self._queue.afull()

    async def _acquire(self) -> _AcquireResult:
        acquisition_id = uuid.uuid4().hex
        async with asyncio.timeout(self.max_acquire_time):
            task = _get_current_task()
            try:
                slot_number = await self._queue.put(
                    _QueueItem(task=task, acquisition_id=acquisition_id)
                )
            except asyncio.CancelledError:
                # Make sure to not leak a slot of the semaphore on cancellation
                await self.__release(acquisition_id)
                raise
        if self.ttl is not None:
            self._queue.add_timer(
                acquisition_id, self.ttl, self._schedule_expire, acquisition_id
            )
        return _AcquireResult(acquisition_id=acquisition_id, slot_number=slot_number)

    async def __release(self, acquisition_id: str) -> _QueueItem | None:
        """Release a slot by acquisition_id (async version)."""
        self._queue.cancel_and_remove_timer(acquisition_id)
        return await self._queue.remove(acquisition_id)

    def _create_background_task(self, coro) -> asyncio.Task:
        """Create a background task and track it to prevent garbage collection."""
        task = asyncio.create_task(coro)
        self.__background_tasks.add(task)
        task.add_done_callback(self.__background_tasks.discard)
        return task

    def _schedule_expire(self, acquisition_id: str) -> None:
        """Schedule the async expiration handler (called from timer callback)."""
        self._create_background_task(self._expire(acquisition_id))

    async def _expire(self, acquisition_id: str) -> None:
        """Handle TTL expiration asynchronously."""
        self._logger.warning("TTL expired => let's release a slot of the semaphore")
        item = await self.__release(acquisition_id)
        if item is not None:
            if self.cancel_task_after_ttl:
                item.task.cancel()

    async def _arelease(self, acquisition_id: str) -> None:
        await self.__release(acquisition_id)

    def _release(self, acquisition_id: str) -> None:
        # Schedule the async release as a background task and return immediately.
        # This matches asyncio.Semaphore.release() behavior which also returns
        # before the effects are fully visible to other waiting tasks.
        # Note: we cannot block/wait here because time.sleep() would block the
        # event loop and prevent the background task from ever executing.
        self._create_background_task(self.__release(acquisition_id))

    @classmethod
    async def get_acquired_stats(
        cls,
        *,
        names: list[str] | None = None,
        limit: int = 100,
        queue_manager: _QueueManager = _DEFAULT_QUEUE_MANAGER,
    ) -> dict[str, SemaphoreStats]:
        return queue_manager.get_acquired_stats(names=names, limit=limit)
