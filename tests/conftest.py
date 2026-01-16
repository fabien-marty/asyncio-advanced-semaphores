import pytest
import stlog

from asyncio_advanced_semaphores.memory.queue import _DEFAULT_QUEUE_MANAGER
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
async def check_queue_manager_is_empty():
    yield
    if not _DEFAULT_QUEUE_MANAGER.is_empty:
        raise Exception("Remaining queues in _DEFAULT_QUEUE_MANAGER")
