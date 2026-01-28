import asyncio
import math

import pytest

from asyncio_advanced_semaphores import MemorySemaphore, RedisSemaphore, Semaphore
from tests.common import (
    assert_locked,
    assert_not_locked,
    worker1,
    worker2,
)


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(name="test-basic", value=1),
        RedisSemaphore(name="test-basic", value=1),
    ],
)
async def test_basic_usage(sem: Semaphore):
    async with sem.cm():
        pass
    async with sem.cm():
        pass


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(name="test-double-acquire", value=1),
        RedisSemaphore(
            name="test-double-acquire",
            value=1,
        ),
    ],
)
async def test_double_acquire(sem: Semaphore):
    result = await sem.acquire()
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.1):
            await sem.acquire()
    await sem.release(result.acquisition_id)


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(name="test-double-release", value=1),
        RedisSemaphore(
            name="test-double-release",
            value=1,
        ),
    ],
)
async def test_double_release(sem: Semaphore):
    result = await sem.acquire()
    await sem.release(result.acquisition_id)
    # Releasing the same acquisition_id again should be a no-op (idempotent)
    await sem.release(result.acquisition_id)


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(name="test-concurrent", value=10, ttl=10),
        RedisSemaphore(
            name="test-concurrent",
            value=10,
            ttl=20,
        ),
    ],
)
async def test_concurrent_usage(sem: Semaphore):
    tasks = [asyncio.create_task(worker1(sem)) for _ in range(1_000)]
    await asyncio.gather(*tasks)


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(
            name="test-acquire-timeout",
            value=1,
            max_acquire_time=0.3,
            ttl=10,
        ),
        RedisSemaphore(
            name="test-acquire-timeout",
            value=1,
            max_acquire_time=0.3,
            ttl=10,
        ),
    ],
)
async def test_acquire_timeout(sem: Semaphore):
    task = asyncio.create_task(worker2(sem))
    await asyncio.sleep(0.05)
    await assert_locked(sem)
    try:
        async with sem.cm():
            raise Exception("should not be able to acquire the semaphore")
    except TimeoutError:
        pass
    await task
    await assert_not_locked(sem)


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(
            name="test-ttl",
            value=1,
            ttl=1,
        ),
        RedisSemaphore(
            name="test-ttl",
            value=1,
            ttl=1,
        ),
    ],
)
async def test_ttl(sem: Semaphore):
    task = asyncio.create_task(worker2(sem, sleep=3.0))
    await asyncio.sleep(0.05)
    await assert_locked(sem)
    await asyncio.sleep(2.0)  # wait for the TTL to expire
    await assert_not_locked(sem)
    await task


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(name="test-timeout-whole-block", value=1),
        RedisSemaphore(
            name="test-timeout-whole-block",
            value=1,
        ),
    ],
)
async def test_timeout_whole_block(sem: Semaphore):
    try:
        async with asyncio.timeout(0.1):
            async with sem.cm():
                await asyncio.sleep(1)
    except TimeoutError:
        pass
    await assert_not_locked(sem)


@pytest.mark.parametrize(
    "sems",
    [
        (
            MemorySemaphore(
                name="test-stats-1",
                value=10,
                ttl=None,
            ),
            MemorySemaphore(
                name="test-stats-2",
                value=20,
                ttl=None,
            ),
        ),
        (
            RedisSemaphore(
                name="test-stats-1",
                value=10,
                ttl=None,
            ),
            RedisSemaphore(
                name="test-stats-2",
                value=20,
                ttl=None,
            ),
        ),
    ],
)
async def test_acquisition_statistics(sems: tuple[Semaphore, Semaphore]):
    # Acquire twice on each semaphore, storing the results
    results1 = [await sem.acquire() for sem in sems]
    results2 = [await sem.acquire() for sem in sems]
    if isinstance(sems[0], MemorySemaphore):
        stats = await MemorySemaphore.get_acquired_stats()
    elif isinstance(sems[0], RedisSemaphore):
        stats = await RedisSemaphore.get_acquired_stats()
    else:
        raise Exception("Unknown semaphore type")
    assert len(stats) == len(sems)
    stats1 = stats[sems[0].name]
    stats2 = stats[sems[1].name]
    assert math.isclose(stats1.acquired_percent, 20.0)
    assert math.isclose(stats2.acquired_percent, 10.0)
    # Release all acquisitions
    for sem, result in zip(sems, results1, strict=True):
        await sem.release(result.acquisition_id)
    for sem, result in zip(sems, results2, strict=True):
        await sem.release(result.acquisition_id)
    if isinstance(sems[0], MemorySemaphore):
        stats = await MemorySemaphore.get_acquired_stats()
    elif isinstance(sems[0], RedisSemaphore):
        stats = await RedisSemaphore.get_acquired_stats()
    else:
        raise Exception("Unknown semaphore type")
    assert len(stats) == 0
