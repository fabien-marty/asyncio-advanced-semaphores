import asyncio
from dataclasses import dataclass, field
from typing import Any

from redis.asyncio import BlockingConnectionPool, Redis

from asyncio_advanced_semaphores.redis.conf import RedisConfig


@dataclass
class RedisClientManager:
    conf: RedisConfig = field(default_factory=RedisConfig)
    _redis_kwargs: dict[str, Any] = field(default_factory=dict)
    _event_loop_id: int | None = None
    _acquire_pool: BlockingConnectionPool | None = None
    _release_pool: BlockingConnectionPool | None = None
    _acquire_client: Redis | None = None
    _release_client: Redis | None = None

    def __post_init__(self):
        self._redis_kwargs = {
            "max_connections": self.conf.max_connections // 2,
            "timeout": None,
            "retry_on_timeout": False,
            "retry_on_error": False,
            "health_check_interval": 10,
            "socket_connect_timeout": self.conf.socket_connect_timeout,
            "socket_timeout": self.conf.socket_timeout,
        }

    def _check_or_set_eventloop_id(self):
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            raise Exception("No running event loop found!") from None
        current_event_loop_id = id(current_loop)
        if self._event_loop_id is None:
            self._event_loop_id = current_event_loop_id
        elif self._event_loop_id != current_event_loop_id:
            raise Exception(
                "Event loop ID mismatch! => Don't share RedisClientManager instances between different event loops!"
            )

    def _get_acquire_pool(self) -> BlockingConnectionPool:
        if self._acquire_pool is None:
            self._acquire_pool = BlockingConnectionPool.from_url(
                self.conf.url, **self._redis_kwargs
            )
        return self._acquire_pool

    def _get_release_pool(self) -> BlockingConnectionPool:
        if self._release_pool is None:
            self._release_pool = BlockingConnectionPool.from_url(
                self.conf.url, **self._redis_kwargs
            )
        return self._release_pool

    def get_acquire_client(self) -> Redis:
        self._check_or_set_eventloop_id()
        if self._acquire_client is None:
            self._acquire_client = Redis.from_pool(self._get_acquire_pool())
        return self._acquire_client

    def get_release_client(self) -> Redis:
        self._check_or_set_eventloop_id()
        if self._release_client is None:
            self._release_client = Redis.from_pool(self._get_release_pool())
        return self._release_client

    async def reset(self):
        self._check_or_set_eventloop_id()
        await self.get_acquire_client().aclose(close_connection_pool=True)
        await self.get_release_client().aclose(close_connection_pool=True)
        self._acquire_pool = None
        self._release_pool = None
        self._acquire_client = None
        self._release_client = None
        self._event_loop_id = None


DEFAULT_REDIS_CLIENT_MANAGER = RedisClientManager()
