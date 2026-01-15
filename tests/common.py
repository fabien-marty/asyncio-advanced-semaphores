import asyncio

from asyncio_advanced_semaphores import Semaphore
from asyncio_advanced_semaphores.redis.client import RedisClientManager

counter = 0


async def worker1(sem: Semaphore):
    global counter
    async with sem as acquisition:
        counter += 1
        if counter > sem.value:
            raise Exception("Concurrent limit exceeded")
        await asyncio.sleep(0.001)
        assert acquisition.acquire_time > 0
        assert acquisition.id
        counter -= 1


async def worker2(sem: Semaphore, sleep: float = 0.5):
    async with sem:
        await asyncio.sleep(sleep)


async def worker3(sem: Semaphore):
    async with sem:
        await asyncio.sleep(3)
        raise Exception("should not happen")


async def locked(sem: Semaphore) -> bool:
    try:
        return sem.locked()
    except NotImplementedError:
        return await sem.alocked()


async def assert_locked(sem: Semaphore):
    assert await locked(sem)


async def assert_not_locked(sem: Semaphore):
    assert not await locked(sem)


def get_new_redis_client_manager() -> RedisClientManager:
    return RedisClientManager()
