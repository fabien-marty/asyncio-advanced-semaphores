import pytest

from asyncio_advanced_semaphores.redis.client import RedisClientManager
from asyncio_advanced_semaphores.redis.conf import RedisConfig


def test_connection_pools_are_split_40_40_20() -> None:
    manager = RedisClientManager(conf=RedisConfig(max_connections=300))

    assert manager._acquire_kwargs["max_connections"] == 120  # noqa: SLF001
    assert manager._release_kwargs["max_connections"] == 120  # noqa: SLF001
    assert manager._watchdog_kwargs["max_connections"] == 60  # noqa: SLF001


def test_max_connections_must_be_greater_than_10() -> None:
    with pytest.raises(ValueError, match="greater than 10"):
        RedisConfig(max_connections=10)
