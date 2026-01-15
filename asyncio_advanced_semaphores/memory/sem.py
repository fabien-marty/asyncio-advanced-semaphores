import asyncio
import time
from dataclasses import dataclass, field

from asyncio_advanced_semaphores.acquisition_id import (
    _new_acquisition_id,
    _pop_acquisition_id,
    _push_acquisition_id,
    _remove_acquisition_id,
    _remove_acquisition_id_any_task,
)
from asyncio_advanced_semaphores.common import (
    Semaphore,
    SemaphoreAcquisition,
    SemaphoreAcquisitionStats,
)
from asyncio_advanced_semaphores.memory.queue import (
    _DEFAULT_QUEUE_MANAGER,
    _QueueItem,
    _QueueManager,
    _QueueWithCreationDate,
)
from asyncio_advanced_semaphores.utils import _get_current_task


@dataclass(kw_only=True, frozen=True)
class MemorySemaphore(Semaphore):
    cancel_task_after_ttl: bool = False
    """If set to True, the task that acquired the slot is cancelled automatically if it's not released within the TTL.
    
    """
    _queue_manager: _QueueManager = field(
        default_factory=lambda: _DEFAULT_QUEUE_MANAGER
    )
    __background_tasks: set[asyncio.Task] = field(default_factory=set)

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

    async def acquire(self) -> SemaphoreAcquisition:
        acquisition_id = _new_acquisition_id()
        before = time.perf_counter()
        async with asyncio.timeout(self.max_acquire_time):
            task = _get_current_task()
            try:
                await self._queue.put(
                    _QueueItem(task=task, acquisition_id=acquisition_id)
                )
            except asyncio.CancelledError:
                # Make sure to not leak a slot of the semaphore on cancellation
                await self._release(acquisition_id)
                raise
        if self.ttl is not None:
            self._queue.add_timer(
                acquisition_id, self.ttl, self._schedule_expire, acquisition_id
            )
        after = time.perf_counter()
        _push_acquisition_id(self.name, acquisition_id)
        return SemaphoreAcquisition(id=acquisition_id, acquire_time=after - before)

    async def _release(self, acquisition_id: str) -> _QueueItem | None:
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
        item = await self._release(acquisition_id)
        if item is not None:
            # Remove the acquisition_id from the tracker so that when the task
            # exits the async with block, it won't try to release again
            _remove_acquisition_id(id(item.task), self.name, acquisition_id)
            if self.cancel_task_after_ttl:
                item.task.cancel()

    async def arelease(self, acquisition_id: str | None = None) -> None:
        if acquisition_id is None:
            acquisition_id = _pop_acquisition_id(self.name)
            if acquisition_id is None:
                return
        else:
            # Remove the explicit acquisition_id from the tracker (may be from any task)
            _remove_acquisition_id_any_task(self.name, acquisition_id)
        await self._release(acquisition_id)

    def release(self, acquisition_id: str | None = None) -> None:
        # If no acquisition_id is provided, we need to pop it from the tracker
        # NOW (in the current task context), not in the background task.
        # Otherwise the background task would look for acquisitions from its own
        # task context, which would be empty.
        if acquisition_id is None:
            acquisition_id = _pop_acquisition_id(self.name)
            if acquisition_id is None:
                return
        else:
            # Remove the explicit acquisition_id from the tracker (may be from any task)
            _remove_acquisition_id_any_task(self.name, acquisition_id)
        # Schedule the async release as a background task and return immediately.
        # This matches asyncio.Semaphore.release() behavior which also returns
        # before the effects are fully visible to other waiting tasks.
        # Note: we cannot block/wait here because time.sleep() would block the
        # event loop and prevent the background task from ever executing.
        self._create_background_task(self._release(acquisition_id))

    @classmethod
    async def get_acquisition_statistics(
        cls,
        *,
        names: list[str] | None = None,
        limit: int = 100,
        queue_manager: _QueueManager = _DEFAULT_QUEUE_MANAGER,
    ) -> dict[str, SemaphoreAcquisitionStats]:
        return queue_manager.get_acquisition_statistics(names=names, limit=limit)
