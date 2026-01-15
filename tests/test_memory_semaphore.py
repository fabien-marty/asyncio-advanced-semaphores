# tests specific to the MemorySemaphore implementation
import asyncio

import pytest

from asyncio_advanced_semaphores import MemorySemaphore
from asyncio_advanced_semaphores.acquisition_id import _pop_acquisition_id
from asyncio_advanced_semaphores.memory.queue import (
    _DEFAULT_QUEUE_MANAGER,
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
    assert sem1.locked()
    assert sem2.locked()
    async with sem2 as acq:
        assert acq.acquire_time > 0.3
    await task
    assert not sem1.locked()
    assert not sem2.locked()


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
        MemorySemaphore(value=0)


async def test_queue_manager_cleanup():
    qm = _QueueManager(empty_queue_max_ttl=0.1)
    sem = MemorySemaphore(value=1, name="test", _queue_manager=qm)
    async with sem:
        pass
    await asyncio.sleep(0.2)
    sem2 = MemorySemaphore(value=1, name="test2", _queue_manager=qm)
    async with sem2:
        pass
    assert qm.get_size() <= 1
    await asyncio.sleep(0.2)
    qm._cleanup_old_empty_queues()  # noqa: SLF001
    assert qm.get_size() == 0


async def test_sync_release():
    """Test that the sync release() method works without deadlocking."""
    sem = MemorySemaphore(value=1)
    await sem.acquire()
    await assert_locked(sem)
    sem.release()
    # Give the background task time to complete
    await asyncio.sleep(0.01)
    await assert_not_locked(sem)


async def test_cross_instance_release_cancels_ttl_timer():
    """Test that releasing with a different instance cancels the TTL timer.

    When acquiring with one MemorySemaphore instance and releasing with another
    (both sharing the same name), the TTL timer should be properly cancelled.
    This prevents false "TTL expired" warnings and avoids leaking background tasks.
    """
    sem1 = MemorySemaphore(name="test", value=1, ttl=1)
    sem2 = MemorySemaphore(name="test", value=1, ttl=1)

    # Acquire with sem1 (this creates a TTL timer)
    acq = await sem1.acquire()
    await assert_locked(sem1)
    await assert_locked(sem2)

    # Verify a timer was created in the shared queue
    queue = _DEFAULT_QUEUE_MANAGER.get_or_create_queue("test", 1)
    assert acq.id in queue._timers  # noqa: SLF001

    # Release with sem2 (timer should be cancelled)
    await sem2.arelease(acq.id)
    # Clean up acquisition tracker since we used explicit ID
    # (normally arelease() without args would pop from tracker automatically)
    _pop_acquisition_id("test")
    await assert_not_locked(sem1)
    await assert_not_locked(sem2)

    # Timer should be removed from the shared queue
    assert acq.id not in queue._timers  # noqa: SLF001

    # Wait for the TTL to pass - the slot should remain free
    # (if the timer wasn't cancelled, _expire would have run on an empty slot)
    await asyncio.sleep(1.1)
    await assert_not_locked(sem1)
    await assert_not_locked(sem2)
