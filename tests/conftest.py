import pytest
import stlog

from asyncio_advanced_semaphores import RedisSemaphore
from asyncio_advanced_semaphores.acquisition_id import _DEFAULT_ACQUISITION_ID_TRACKER
from asyncio_advanced_semaphores.memory.queue import _DEFAULT_QUEUE_MANAGER
from asyncio_advanced_semaphores.redis.ping import _PING_TASKS_MANAGER
from tests.common import get_new_redis_client_manager


@pytest.fixture(autouse=True)
def setup_stlog():
    stlog.setup(level="DEBUG")


@pytest.fixture(autouse=True)
async def cleanup_redis_and_check_remaining_keys():
    client = get_new_redis_client_manager().get_acquire_client()
    await client.flushall()
    yield
    remaining_keys = await client.keys("*")
    remaining_keys = [
        key.decode() for key in remaining_keys if b"semaphore_max" not in key
    ]
    if len(remaining_keys) > 0:
        raise Exception(f"Remaining keys: {remaining_keys}")


@pytest.fixture(autouse=True)
async def check_ping_tags_manager_is_empty_and_clean():
    yield
    if not _PING_TASKS_MANAGER.is_empty:
        raise Exception("Remaining tasks in _PING_TASKS_MANAGER")
    await RedisSemaphore.stop_all()


@pytest.fixture(autouse=True)
async def check_queue_manager_is_empty():
    yield
    if not _DEFAULT_QUEUE_MANAGER.is_empty:
        raise Exception("Remaining queues in _DEFAULT_QUEUE_MANAGER")


@pytest.fixture(autouse=True)
async def check_acquisition_id_tracker_is_empty():
    yield
    if not _DEFAULT_ACQUISITION_ID_TRACKER.is_empty:
        raise Exception("Remaining acquisition ids in _DEFAULT_ACQUISITION_ID_TRACKER")
