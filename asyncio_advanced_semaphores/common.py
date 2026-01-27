from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field

import stlog

_DEFAULT_LOGGER = stlog.getLogger("asyncio_advanced_semaphores")


@dataclass
class SemaphoreStats:
    """Statistics for a semaphore's current acquisition state.

    This dataclass provides information about how many slots are currently
    acquired in a semaphore and the total number of available slots.

    Attributes:
        acquired_slots: The number of slots currently held by tasks.
        max_slots: The maximum number of slots available for this semaphore.

    Example:
        ```python
        stats = await MySemaphore.get_acquired_stats(names=["my-sem"])
        if "my-sem" in stats:
            print(f"Usage: {stats['my-sem'].acquired_percent:.1f}%")
        ```

    """

    acquired_slots: int
    """The number of slots currently held by tasks."""

    max_slots: int
    """The maximum number of slots available for this semaphore."""

    @property
    def acquired_percent(self) -> float:
        """Calculate the percentage of slots currently acquired.

        Returns:
            The percentage of acquired slots (0.0 to 100.0).

        Raises:
            AssertionError: If max_slots is not greater than 0.

        """
        assert self.max_slots > 0
        return 100.0 * self.acquired_slots / self.max_slots


@dataclass
class AcquisitionResult:
    """Result of a semaphore acquisition.

    This dataclass contains the acquisition ID and the slot number.

    Attributes:
        acquisition_id: The unique identifier for the acquisition.
        slot_number: The number of the slot acquired (starting from 0).

    """

    acquisition_id: str
    """The unique identifier for the acquisition."""

    slot_number: int
    """The number of the slot acquired (starting from 0)."""


class SemaphoreContextManager(AbstractAsyncContextManager[AcquisitionResult]):
    __semaphore: Semaphore | None
    __result: AcquisitionResult | None

    def __init__(self, semaphore: Semaphore):
        self.__semaphore = semaphore
        self.__result = None

    async def __aenter__(self) -> AcquisitionResult:
        """Enter the async context manager by acquiring the semaphore.

        Returns:
            An `AcquisitionResult` object containing the acquisition ID and the slot number.

        """
        if self.__result is not None:
            raise Exception(
                "MISUSE: Semaphore context manager already acquired, it looks like you shared the output of the cm() method => never do that!"
            )
        if self.__semaphore is None:
            raise Exception(
                "MISUSE: You reused the context manager after it was already exited => never do that!"
            )
        self.__result = await self.__semaphore.acquire()
        return self.__result

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        """Exit the async context manager by releasing the semaphore.

        This method is called when exiting an `async with` block. It releases
        the slot that was acquired by `__aenter__()`.

        Args:
            exc_type: The exception type if an exception was raised, else None.
            exc_value: The exception instance if an exception was raised, else None.
            traceback: The traceback if an exception was raised, else None.

        Note:
            The semaphore is released regardless of whether an exception occurred.
            Exceptions are not suppressed (this method returns None).

        """
        if self.__semaphore is None:
            raise Exception(
                "MISUSE: You reused the context manager after it was already exited => never do that!"
            )
        if self.__result is None:
            raise Exception(
                "MISUSE: You didn't acquire the semaphore before exiting the context manager => never do that!"
            )
        await self.__semaphore.release(self.__result.acquisition_id)
        self.__result = None
        self.__semaphore = None


@dataclass(kw_only=True)
class Semaphore(ABC):
    name: str
    """Name of the semaphore.
    
    If you create two semaphores with the same name, they will share the same slots.
    
    """

    value: int
    """The (maximum) number of slots available for this semaphore."""

    max_acquire_time: float | None = None
    """The maximum time (in seconds) to acquire the semaphore.
    
    If None => no timeout is applied.

    If we can't acquire the semaphore within the timeout, an TimeoutError is raised.
    
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
    def _get_type(self) -> str:
        """Returns the type of the semaphore implementation."""
        pass  # pragma: no cover

    @abstractmethod
    async def locked(self) -> bool:
        """Returns True if semaphore cannot be acquired immediately."""
        pass  # pragma: no cover

    @abstractmethod
    async def _acquire(self) -> AcquisitionResult:
        pass  # pragma: no cover

    async def acquire(self) -> AcquisitionResult:
        """Acquire the semaphore (manually).

        This method acquires one slot from the semaphore. If no slots are
        available, it waits until one becomes available or until
        `max_acquire_time` is exceeded (if set).

        When acquired with this method, you have to release the slot manually
        with `release()` method.

        Returns:
            An `AcquisitionResult` object containing the acquisition ID and the slot number.

        Raises:
            TimeoutError: If `max_acquire_time` is set and the
                semaphore cannot be acquired within that time.

        Example:
            ```python
            sem = MySemaphore(name="my-sem", value=2)
            result = await sem.acquire()
            try:
                # critical section
                pass
            finally:
                await sem.release(result.acquisition_id)
            ```

        """
        self._logger.debug(
            "Acquiring semaphore...",
            semaphore_name=self.name,
            type=self._get_type(),
            max_slots=self.value,
        )
        before = time.perf_counter()
        result = await self._acquire()
        acquire_time = time.perf_counter() - before
        self._logger.info(
            "Acquisition successful",
            semaphore_name=self.name,
            acquire_time=acquire_time,
            slot_number=result.slot_number,
            max_slots=self.value,
            type=self._get_type(),
        )
        return result

    @abstractmethod
    async def _release(self, acquisition_id: str) -> None:
        pass  # pragma: no cover

    async def release(self, acquisition_id: str) -> None:
        """Release the semaphore (manually).

        This method releases one slot back to the semaphore.

        Example:
            ```python
            sem = MySemaphore(name="my-sem", value=2)
            result = await sem.acquire()
            try:
                # critical section
                pass
            finally:
                await sem.release(result.acquisition_id)
            ```

        """
        self._logger.debug(
            "Releasing semaphore...",
            semaphore_name=self.name,
            acquisition_id=acquisition_id,
            type=self._get_type(),
        )
        await self._release(acquisition_id)
        self._logger.info(
            "Release successful",
            semaphore_name=self.name,
            type=self._get_type(),
        )

    def cm(self) -> AbstractAsyncContextManager[AcquisitionResult]:
        return SemaphoreContextManager(self)

    @classmethod
    @abstractmethod
    async def get_acquired_stats(
        cls, *, names: list[str] | None = None, limit: int = 100
    ) -> dict[str, SemaphoreStats]:
        """Get acquisition statistics for semaphores.

        This class method retrieves the current state of semaphore acquisitions,
        showing how many slots are currently held for each semaphore.

        Note: not acquired semaphores (with 0 acquired slots) are not returned.

        Args:
            names: Optional list of semaphore names to query. If None, returns
                statistics for all known semaphores (up to `limit`).
            limit: Maximum number of semaphores to return when `names` is None.
                Results are sorted by acquired percentage (highest first).
                Defaults to 100.

        Returns:
            A dictionary mapping semaphore names to their SemaphoreStats,
            sorted by acquired percentage in descending order.

        Example:
            ```python
            # Get stats for specific semaphores
            stats = await MySemaphore.get_acquired_stats(names=["api-limit", "db-pool"])
            for name, stat in stats.items():
                print(f"{name}: {stat.acquired_slots}/{stat.max_slots} ({stat.acquired_percent:.1f}%)")

            # Get top 10 most utilized semaphores
            stats = await MySemaphore.get_acquired_stats(limit=10)
            ```

        """
        pass  # pragma: no cover
