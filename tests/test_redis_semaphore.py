import time

from asyncio_advanced_semaphores import RedisSemaphore
from tests.common import get_new_redis_client_manager


async def test_redis_client_manager():
    redis_client_manager = get_new_redis_client_manager()
    await redis_client_manager.reset()


async def test_with_bad_heartbeat():
    async def bad_ping(_: str) -> None:
        return

    redis_client_manager = get_new_redis_client_manager()

    sem1 = RedisSemaphore(
        name="foo",
        value=1,
        ttl=60,
        heartbeat_max_interval=1,
        client_manager=redis_client_manager,
        _overriden_ping_func=bad_ping,
    )
    sem2 = RedisSemaphore(
        name="foo",
        value=1,
        ttl=60,
        client_manager=redis_client_manager,
    )

    acq1 = await sem1.acquire()  # it will expire after 1 second (because no heartbeat)
    before = time.perf_counter()
    acq2 = await sem2.acquire()
    assert acq2.id != acq1.id
    after = time.perf_counter()
    assert after - before > 0.9
    await sem2.arelease(acq2.id)
    await sem1.arelease(acq1.id)
