import logging
import time
import uuid
from abc import ABC, abstractmethod
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
class _AcquireResult:
    acquisition_id: str
    slot_number: int


@dataclass(kw_only=True)
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

    __acquisition_id_stack: list[str] = field(default_factory=list)

    def __post_init__(self):
        if self.value <= 0:
            raise Exception("Semaphore value can't be <= 0")

    @abstractmethod
    def supports_multiple_simultaneous_acquisitions(self) -> bool:
        """Returns True if the semaphore supports multiple simultaneous acquisitions of the same semaphore/object.

        It depends on the settings of the semaphore.

        """
        pass  # pragma: no cover

    @abstractmethod
    def _get_type(self) -> str:
        """Returns the type of the semaphore implementation."""
        pass  # pragma: no cover

    @abstractmethod
    def locked(self):
        """Returns True if semaphore cannot be acquired immediately."""
        pass  # pragma: no cover

    @abstractmethod
    async def alocked(self) -> bool:
        """Returns True if semaphore cannot be acquired immediately."""
        pass  # pragma: no cover

    @abstractmethod
    async def _acquire(self) -> _AcquireResult:
        """Acquire the semaphore."""
        pass  # pragma: no cover

    async def acquire(self) -> bool:
        """Acquire the semaphore.

        This method acquires one slot from the semaphore. If no slots are
        available, it waits until one becomes available or until
        `max_acquire_time` is exceeded (if set).

        The acquisition is tracked internally using an acquisition ID stack,
        allowing proper pairing with subsequent `release()` or `arelease()` calls.

        Returns:
            True when the semaphore is successfully acquired.

        Raises:
            asyncio.TimeoutError: If `max_acquire_time` is set and the
                semaphore cannot be acquired within that time.

        Example:
            ```python
            sem = MySemaphore(value=2)
            await sem.acquire()
            try:
                # critical section
                pass
            finally:
                await sem.arelease()
            ```

        """
        before = time.perf_counter()
        if (
            len(self.__acquisition_id_stack) > 0
            and not self.supports_multiple_simultaneous_acquisitions()
        ):
            raise Exception(
                "This semaphore doesn't support multiple simultaneous acquisitions (with selected settings) => change the settings or use multiple semaphore objects with the same name."
            )
        result = await self._acquire()
        self.__acquisition_id_stack.append(result.acquisition_id)
        acquire_time = time.perf_counter() - before
        self._logger.info(
            "Acquisition successful",
            semaphore_name=self.name,
            acquire_time=acquire_time,
            slot_number=result.slot_number,
            max_slots=self.value,
            type=self._get_type(),
        )
        return True

    @abstractmethod
    def _release(self, acquisition_id: str) -> None:
        """Release the semaphore."""
        pass  # pragma: no cover

    @abstractmethod
    async def _arelease(self, acquisition_id: str) -> None:
        """Release the semaphore."""
        pass  # pragma: no cover

    def release(self) -> None:
        """Release the semaphore synchronously.

        This method releases one slot back to the semaphore. It uses the
        most recent acquisition ID from the internal stack (LIFO order).

        If no acquisition is currently held (empty stack), this method
        does nothing silently.

        Note:
            For semaphore implementations that require async cleanup
            (e.g., Redis-backed semaphores), prefer using `arelease()` instead.

        Example:
            ```python
            sem = MySemaphore(value=2)
            await sem.acquire()
            try:
                # critical section
                pass
            finally:
                sem.release()
            ```

        """
        try:
            acquisition_id = self.__acquisition_id_stack.pop()
        except IndexError:
            return
        self._release(acquisition_id)
        self._logger.info(
            "Release successful",
            semaphore_name=self.name,
            type=self._get_type(),
        )

    async def arelease(self) -> None:
        """Release the semaphore asynchronously.

        This method releases one slot back to the semaphore. It uses the
        most recent acquisition ID from the internal stack (LIFO order).

        If no acquisition is currently held (empty stack), this method
        does nothing silently.

        This is the preferred release method for semaphore implementations
        that require async operations (e.g., Redis-backed semaphores).

        Example:
            ```python
            sem = MySemaphore(value=2)
            await sem.acquire()
            try:
                # critical section
                pass
            finally:
                await sem.arelease()
            ```

        """
        try:
            acquisition_id = self.__acquisition_id_stack.pop()
        except IndexError:
            return
        await self._arelease(acquisition_id)

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

    async def __aenter__(self) -> bool:
        """Enter the async context manager by acquiring the semaphore.

        This method is called when entering an `async with` block. It acquires
        one slot from the semaphore.

        Returns:
            True when the semaphore is successfully acquired.

        Raises:
            asyncio.TimeoutError: If `max_acquire_time` is set and the
                semaphore cannot be acquired within that time.

        Example:
            ```python
            sem = MySemaphore(value=2)
            async with sem:
                # critical section - semaphore is held
                pass
            # semaphore is automatically released here
            ```

        """
        return await self.acquire()

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
        await self.arelease()
