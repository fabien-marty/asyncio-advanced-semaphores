import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from asyncio_advanced_semaphores.utils import _DEFAULT_LOGGER


@dataclass
class SemaphoreAcquisitionStats:
    acquired_slots: int
    max_slots: int

    @property
    def acquired_percent(self) -> float:
        assert self.max_slots > 0
        return 100.0 * self.acquired_slots / self.max_slots


@dataclass
class SemaphoreAcquisition:
    """Semaphore acquisition."""

    id: str
    """Acquisition unique identifier."""

    acquire_time: float
    """The time (in seconds) it took to acquire the semaphore."""

    def __bool__(self) -> bool:
        """Returns True to emulate asyncio.Semaphore interface for acquire()."""
        return True


@dataclass(kw_only=True, frozen=True)
class Semaphore(ABC):
    name: str = field(default_factory=lambda: uuid.uuid4().hex)
    """Name of the semaphore.
    
    If not set, a random UUID is used. If you create two semaphores with the same name, they will share the same slots.
    In that case, this is like you share the same semaphore.
    
    """

    value: int
    """The number of slots available for this semaphore."""

    max_acquire_time: float | None = None
    """The maximum time (in seconds) to acquire the semaphore.
    
    If None => no timeout is applied.

    If we can't acquire the semaphore within the timeout, an asyncio.TimeoutError is raised.
    
    """

    ttl: int | None = None
    """Semaphore time to live (in seconds) after acquisition.
    
    After this time, the acquired slot is released automatically.

    Note: the task that acquired the slot is not cancelled automatically
    (see `cancel_task_after_ttl` parameter to enable this behaviour for some semaphore implementations).

    """

    _logger: logging.LoggerAdapter = field(default_factory=lambda: _DEFAULT_LOGGER)

    def __post_init__(self):
        if self.value <= 0:
            raise Exception("Semaphore value can't be <= 0")

    @abstractmethod
    def locked(self):
        """Returns True if semaphore cannot be acquired immediately."""
        pass  # pragma: no cover

    @abstractmethod
    async def alocked(self) -> bool:
        """Returns True if semaphore cannot be acquired immediately."""
        pass  # pragma: no cover

    @abstractmethod
    async def acquire(self) -> SemaphoreAcquisition:
        """Acquire the semaphore."""
        pass  # pragma: no cover

    @abstractmethod
    def release(self, acquisition_id: str | None = None) -> None:
        """Release the semaphore."""
        pass  # pragma: no cover

    @abstractmethod
    async def arelease(self, acquisition_id: str | None = None) -> None:
        """Release the semaphore."""
        pass  # pragma: no cover

    @classmethod
    @abstractmethod
    async def get_acquisition_statistics(
        cls, *, names: list[str] | None = None, limit: int = 100
    ) -> dict[str, SemaphoreAcquisitionStats]:
        pass  # pragma: no cover

    async def __aenter__(self) -> SemaphoreAcquisition:
        return await self.acquire()

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.arelease()
