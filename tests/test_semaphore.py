import asyncio
import math

import pytest

from asyncio_advanced_semaphores import MemorySemaphore, RedisSemaphore, Semaphore
from tests.common import (
    assert_locked,
    assert_not_locked,
    get_new_redis_client_manager,
    worker1,
    worker2,
)


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(value=1),
        RedisSemaphore(value=1, client_manager=get_new_redis_client_manager()),
    ],
)
async def test_basic_usage(sem: Semaphore):
    async with sem as acquisition:
        assert acquisition.acquire_time > 0
    async with sem as acquisition:
        assert acquisition.acquire_time > 0


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(value=1),
        RedisSemaphore(value=1, client_manager=get_new_redis_client_manager()),
    ],
)
async def test_double_acquire(sem: Semaphore):
    await sem.acquire()
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.1):
            await sem.acquire()
    await sem.arelease()


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(value=1),
        RedisSemaphore(value=1, client_manager=get_new_redis_client_manager()),
    ],
)
async def test_double_release(sem: Semaphore):
    await sem.acquire()
    await sem.arelease()
    await sem.arelease()


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(value=10, ttl=10),
        RedisSemaphore(
            value=10,
            ttl=20,
            client_manager=get_new_redis_client_manager(),
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
            value=1,
            max_acquire_time=0.3,
            ttl=10,
        ),
        RedisSemaphore(
            value=1,
            max_acquire_time=0.3,
            ttl=10,
            client_manager=get_new_redis_client_manager(),
        ),
    ],
)
async def test_acquire_timeout(sem: Semaphore):
    task = asyncio.create_task(worker2(sem))
    await asyncio.sleep(0.05)
    await assert_locked(sem)
    try:
        async with sem:
            raise Exception("should not be able to acquire the semaphore")
    except TimeoutError:
        pass
    await task
    await assert_not_locked(sem)


@pytest.mark.parametrize(
    "sem",
    [
        MemorySemaphore(
            value=1,
            ttl=1,
        ),
        RedisSemaphore(
            value=1,
            ttl=1,
            client_manager=get_new_redis_client_manager(),
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
        MemorySemaphore(value=1),
        RedisSemaphore(
            value=1,
            client_manager=get_new_redis_client_manager(),
        ),
    ],
)
async def test_timeout_whole_block(sem: Semaphore):
    try:
        async with asyncio.timeout(0.1):
            async with sem:
                await asyncio.sleep(1)
    except TimeoutError:
        pass
    await assert_not_locked(sem)


@pytest.mark.parametrize(
    "sems",
    [
        (
            MemorySemaphore(
                value=10,
                ttl=10,
            ),
            MemorySemaphore(
                value=20,
                ttl=10,
            ),
        ),
        (
            RedisSemaphore(
                value=10,
                client_manager=get_new_redis_client_manager(),
                ttl=10,
            ),
            RedisSemaphore(
                value=20,
                client_manager=get_new_redis_client_manager(),
                ttl=10,
            ),
        ),
    ],
)
async def test_acquisition_statistics(sems: tuple[Semaphore, Semaphore]):
    tasks = [asyncio.create_task(sem.acquire()) for sem in sems]
    acq1 = await asyncio.gather(*tasks)
    tasks = [
        asyncio.create_task(sem.acquire()) for sem in sems
    ]  # let's acquire two times
    acq2 = await asyncio.gather(*tasks)
    if isinstance(sems[0], MemorySemaphore):
        stats = await MemorySemaphore.get_acquisition_statistics()
    elif isinstance(sems[0], RedisSemaphore):
        stats = await RedisSemaphore.get_acquisition_statistics(
            client_manager=sems[0].client_manager
        )
    else:
        raise Exception("Unknown semaphore type")
    assert len(stats) == len(sems)
    stats1 = stats[sems[0].name]
    stats2 = stats[sems[1].name]
    assert math.isclose(stats1.acquired_percent, 20.0)
    assert math.isclose(stats2.acquired_percent, 10.0)
    tasks = [
        asyncio.create_task(sem.arelease(acquisition_id=acq.id))
        for sem, acq in zip(sems, acq1, strict=True)
    ]
    await asyncio.gather(*tasks)
    tasks = [
        asyncio.create_task(sem.arelease(acquisition_id=acq.id))
        for sem, acq in zip(sems, acq2, strict=True)
    ]
    await asyncio.gather(*tasks)
    if isinstance(sems[0], MemorySemaphore):
        stats = await MemorySemaphore.get_acquisition_statistics()
    elif isinstance(sems[0], RedisSemaphore):
        stats = await RedisSemaphore.get_acquisition_statistics()
    else:
        raise Exception("Unknown semaphore type")
    assert len(stats) == 0
