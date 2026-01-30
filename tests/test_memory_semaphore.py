# tests specific to the MemorySemaphore implementation
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from asyncio_advanced_semaphores import MemorySemaphore
from asyncio_advanced_semaphores.memory.queue import (
    _QueueManager,
)
from tests.common import (
    assert_locked,
    assert_not_locked,
    worker2,
    worker3,
)


async def test_memory_semaphore_with_explicit_name():
    sem1 = MemorySemaphore(value=1, name="test")
    sem2 = MemorySemaphore(value=1, name="test")
    task = asyncio.create_task(worker2(sem1))
    await asyncio.sleep(0.05)
    assert await sem1.locked()
    assert await sem2.locked()
    async with sem2.cm():
        pass
    await task
    assert not await sem1.locked()
    assert not await sem2.locked()


async def test_memory_semaphore_cancel_task_after_ttl():
    sem = MemorySemaphore(
        name="test",
        value=1,
        ttl=1,
        cancel_task_after_ttl=True,
    )
    task = asyncio.create_task(worker3(sem))
    await asyncio.sleep(0.05)
    await assert_locked(sem)
    await asyncio.sleep(2.0)  # wait for the TTL to expire
    await assert_not_locked(sem)
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_wrong_semaphore_value():
    with pytest.raises(Exception):
        MemorySemaphore(name="test", value=0)


async def test_queue_manager_cleanup():
    qm = _QueueManager(empty_queue_max_ttl=0.1)
    sem = MemorySemaphore(value=1, name="test", _queue_manager=qm)
    async with sem.cm():
        pass
    await asyncio.sleep(0.2)
    sem2 = MemorySemaphore(value=1, name="test2", _queue_manager=qm)
    async with sem2.cm():
        pass
    assert qm.get_size() <= 1
    await asyncio.sleep(0.2)
    qm._cleanup_old_empty_queues()  # noqa: SLF001
    assert qm.get_size() == 0


async def test_manual_acquire_release():
    """Test manual acquire/release with acquisition_id."""
    sem = MemorySemaphore(name="test", value=1)
    result = await sem.acquire()
    await assert_locked(sem)
    await sem.release(result.acquisition_id)
    await assert_not_locked(sem)


async def test_queue_manager_maxsize_change_when_empty():
    """Test that QueueManager allows maxsize change when queue is empty."""
    qm = _QueueManager()
    name = "test-maxsize-change"

    # Create a semaphore with value=1, acquire and release it
    sem1 = MemorySemaphore(value=1, name=name, _queue_manager=qm)
    async with sem1.cm():
        pass

    # Now create a semaphore with a different value - should work because queue is empty
    sem2 = MemorySemaphore(value=2, name=name, _queue_manager=qm)
    async with sem2.cm():
        pass

    # Verify the new maxsize is in effect
    assert qm.get_size() == 1


async def test_queue_manager_maxsize_change_when_not_empty():
    """Test that QueueManager raises exception when maxsize changes on non-empty queue."""
    qm = _QueueManager()
    name = "test-maxsize-mismatch"

    sem1 = MemorySemaphore(value=1, name=name, _queue_manager=qm)
    result = await sem1.acquire()

    # Try to use a semaphore with a different value while the first one holds the lock
    # The exception is raised when the queue is accessed (lazy access via _queue property)
    sem2 = MemorySemaphore(value=2, name=name, _queue_manager=qm)
    with pytest.raises(Exception, match="maxsize mismatch"):
        await sem2.locked()

    # Cleanup
    await sem1.release(result.acquisition_id)


async def test_thread_safety_multple_threads_same_semaphore():
    """Test thread-safety with multiple threads acquiring the same semaphore name.

    Each thread runs its own event loop and tries to acquire the same
    semaphore. The semaphore has value=1, so only one thread can hold it
    at a time. We verify proper synchronization by tracking concurrent
    acquisitions.
    """
    qm = _QueueManager()
    sem_name = "thread-safe-test"
    sem_value = 1
    acquisitions_per_thread = 100
    number_of_threads = 10

    # Shared state protected by a lock
    lock = threading.Lock()
    concurrent_count = 0
    max_concurrent = 0
    errors: list[str] = []

    def thread_worker() -> None:
        """Run async semaphore acquisitions in a new event loop."""

        async def acquire_multiple_times() -> None:
            nonlocal concurrent_count, max_concurrent
            sem = MemorySemaphore(name=sem_name, value=sem_value, _queue_manager=qm)

            for _ in range(acquisitions_per_thread):
                async with sem.cm():
                    with lock:
                        concurrent_count += 1
                        max_concurrent = max(max_concurrent, concurrent_count)
                        if concurrent_count > sem_value:
                            errors.append(
                                f"Concurrent limit exceeded: {concurrent_count} > {sem_value}"
                            )
                    # Hold the semaphore briefly
                    await asyncio.sleep(0.01)
                    with lock:
                        concurrent_count -= 1

        asyncio.run(acquire_multiple_times())

    # Run 2 threads concurrently
    with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
        futures = [executor.submit(thread_worker) for _ in range(number_of_threads)]
        for future in futures:
            future.result()  # Re-raise any exceptions

    # Verify no errors occurred
    assert not errors, f"Thread-safety errors: {errors}"
    # Verify semaphore limit was respected
    assert max_concurrent <= sem_value, f"Max concurrent {max_concurrent} > {sem_value}"
    # Verify the semaphore was actually contested (at least once both threads were active)
    assert max_concurrent == sem_value, "Semaphore was never fully utilized"
