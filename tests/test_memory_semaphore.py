# tests specific to the MemorySemaphore implementation
import asyncio

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
    assert sem1.locked()
    assert sem2.locked()
    async with sem2:
        pass
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
