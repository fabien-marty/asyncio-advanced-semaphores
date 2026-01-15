import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field

from asyncio_advanced_semaphores.utils import _get_current_task


@dataclass
class _AcquisitionIdTracker:
    # (task id, name) => acquisition id
    _ids: dict[tuple[int, str], list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def push_acquisition_id(self, name: str, acquisition_id: str) -> None:
        with self._lock:
            current_task = _get_current_task()
            self._ids[(id(current_task), name)].append(acquisition_id)

    def pop_acquisition_id(self, name: str) -> str | None:
        with self._lock:
            current_task = _get_current_task()
            key = (id(current_task), name)
            if key not in self._ids:
                return None
            try:
                result = self._ids[key].pop()
                # Clean up empty list to prevent memory leak
                if not self._ids[key]:
                    del self._ids[key]
                return result
            except IndexError:
                # List was already empty, clean it up
                del self._ids[key]
                return None

    def remove_acquisition_id(
        self, task_id: int, name: str, acquisition_id: str
    ) -> bool:
        """Remove a specific acquisition_id from a task's stack.

        This is used when TTL expires and we need to remove the acquisition_id
        from the tracker even though we're not in the original task's context.

        Args:
            task_id: The id of the task that acquired the semaphore.
            name: The semaphore name.
            acquisition_id: The acquisition ID to remove.

        Returns:
            True if the acquisition_id was found and removed, False otherwise.
        """
        with self._lock:
            key = (task_id, name)
            if key not in self._ids:
                return False
            try:
                self._ids[key].remove(acquisition_id)
                # Clean up empty list to prevent memory leak
                if not self._ids[key]:
                    del self._ids[key]
                return True
            except ValueError:
                return False

    def remove_acquisition_id_any_task(self, name: str, acquisition_id: str) -> bool:
        """Remove a specific acquisition_id from any task's stack.

        This searches all task entries for the given semaphore name and removes
        the acquisition_id if found. Used when releasing with an explicit
        acquisition_id from a different task than the one that acquired it.

        Args:
            name: The semaphore name.
            acquisition_id: The acquisition ID to remove.

        Returns:
            True if the acquisition_id was found and removed, False otherwise.
        """
        with self._lock:
            for key, ids in list(self._ids.items()):
                _, key_name = key
                if key_name != name:
                    continue
                try:
                    ids.remove(acquisition_id)
                    # Clean up empty list to prevent memory leak
                    if not ids:
                        del self._ids[key]
                    return True
                except ValueError:
                    continue
            return False

    @property
    def is_empty(self) -> bool:
        return len(self._ids) == 0


_DEFAULT_ACQUISITION_ID_TRACKER = _AcquisitionIdTracker()


def _new_acquisition_id() -> str:
    return uuid.uuid4().hex


def _push_acquisition_id(name: str, acquisition_id: str) -> None:
    _DEFAULT_ACQUISITION_ID_TRACKER.push_acquisition_id(name, acquisition_id)


def _pop_acquisition_id(name: str) -> str | None:
    return _DEFAULT_ACQUISITION_ID_TRACKER.pop_acquisition_id(name)


def _remove_acquisition_id(task_id: int, name: str, acquisition_id: str) -> bool:
    return _DEFAULT_ACQUISITION_ID_TRACKER.remove_acquisition_id(
        task_id, name, acquisition_id
    )


def _remove_acquisition_id_any_task(name: str, acquisition_id: str) -> bool:
    return _DEFAULT_ACQUISITION_ID_TRACKER.remove_acquisition_id_any_task(
        name, acquisition_id
    )
