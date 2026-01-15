import asyncio
import functools
import threading
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _PingTask:
    semaphore_name: str
    asyncio_task: asyncio.Task


@dataclass
class _PingTasksManager:
    # acquisition_id -> task
    _tasks: dict[str, _PingTask] = field(default_factory=dict)

    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _done_callback(self, _: asyncio.Task, acquisition_id: str) -> None:
        with self._lock:
            try:
                self._tasks.pop(acquisition_id)
            except KeyError:
                pass

    def create_and_add_ping_task(
        self,
        *,
        acquisition_id: str,
        callback: Callable[[str], Coroutine[Any, Any, None]],
        semaphore_name: str,
    ) -> None:
        task = asyncio.create_task(callback(acquisition_id))
        with self._lock:
            ping_task = _PingTask(
                semaphore_name=semaphore_name,
                asyncio_task=task,
            )
            self._tasks[acquisition_id] = ping_task
        task.add_done_callback(
            functools.partial(self._done_callback, acquisition_id=acquisition_id)
        )

    def cancel_task(self, acquisition_id: str) -> None:
        with self._lock:
            task = self._tasks.pop(acquisition_id, None)
            if task:
                task.asyncio_task.cancel()

    def get_semaphore_names(self) -> set[str]:
        with self._lock:
            return {task.semaphore_name for task in self._tasks.values()}

    @property
    def is_empty(self) -> bool:
        return len(self._tasks) == 0

    async def clean(self) -> None:
        # Extract tasks and clear dict while holding the lock,
        # then await outside the lock to avoid deadlock with done_callback
        with self._lock:
            tasks_to_clean = list(self._tasks.values())
            self._tasks.clear()
        for task in tasks_to_clean:
            task.asyncio_task.cancel()
            try:
                await task.asyncio_task
            except BaseException:
                pass


_PING_TASKS_MANAGER = _PingTasksManager()
