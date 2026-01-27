import asyncio
import threading
from dataclasses import dataclass, field
from typing import Any

from redis.asyncio import BlockingConnectionPool, Redis

from asyncio_advanced_semaphores.redis.conf import RedisConfig

_CLIENT_MANAGERS: dict[tuple[int, int], "RedisClientManager"] = {}
_CLIENT_MANAGERS_LOCK: threading.Lock = threading.Lock()


def _get_event_loop_id() -> int:
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        raise Exception("No running event loop found!") from None
    return id(current_loop)


def _get_client_manager(config: RedisConfig) -> "RedisClientManager":
    event_loop_id = _get_event_loop_id()
    config_hash = hash(config)
    with _CLIENT_MANAGERS_LOCK:
        if (event_loop_id, config_hash) not in _CLIENT_MANAGERS:
            _CLIENT_MANAGERS[(event_loop_id, config_hash)] = RedisClientManager(
                conf=config
            )
        return _CLIENT_MANAGERS[(event_loop_id, config_hash)]


@dataclass
class RedisClientManager:
    """Manages Redis client connections with separate pools for acquire and release operations.

    This manager maintains two distinct connection pools to prevent deadlocks:
    - **Acquire pool**: Used for operations that may block (e.g., BLPOP during semaphore acquisition)
    - **Release pool**: Used for non-blocking release operations

    The separation ensures that release operations can always proceed even when all
    acquire connections are blocked waiting for semaphore slots.

    Key features:
    - **Dual connection pools**: Prevents deadlocks between acquire and release operations
    - **Lazy initialization**: Pools and clients are created on first use

    Attributes:
        conf: Redis configuration (URL, timeouts, max connections, etc.)

    Example:
        ```python
        from asyncio_advanced_semaphores.redis import RedisClientManager, RedisConfig

        # Create with default configuration
        manager = RedisClientManager()

        # Or with custom configuration
        config = RedisConfig(url="redis://myhost:6379", max_connections=100)
        manager = RedisClientManager(conf=config)

        # Get clients for Redis operations
        acquire_client = manager.get_acquire_client()
        release_client = manager.get_release_client()

        # Clean up when done
        await manager.reset()
        ```
    """

    conf: RedisConfig = field(default_factory=RedisConfig)
    _redis_kwargs: dict[str, Any] = field(default_factory=dict)
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

    def _get_acquire_pool(self) -> BlockingConnectionPool:
        """Get or create the connection pool for acquire operations.

        This pool is dedicated to acquire operations which may block (e.g., BLPOP).

        Returns:
            The blocking connection pool for acquire operations.
        """
        if self._acquire_pool is None:
            self._acquire_pool = BlockingConnectionPool.from_url(
                self.conf.url, **self._redis_kwargs
            )
        return self._acquire_pool

    def _get_release_pool(self) -> BlockingConnectionPool:
        """Get or create the connection pool for release operations.

        This pool is dedicated to release operations, kept separate from
        acquire operations to prevent deadlocks.

        Returns:
            The blocking connection pool for release operations.
        """
        if self._release_pool is None:
            self._release_pool = BlockingConnectionPool.from_url(
                self.conf.url, **self._redis_kwargs
            )
        return self._release_pool

    def get_acquire_client(self) -> Redis:
        """Get or create the Redis client for acquire operations.

        This client uses the acquire connection pool, which is dedicated to
        operations that may block (e.g., BLPOP during semaphore acquisition).

        Returns:
            Redis client for acquire operations.
        """
        if self._acquire_client is None:
            self._acquire_client = Redis.from_pool(self._get_acquire_pool())
        return self._acquire_client

    def get_release_client(self) -> Redis:
        """Get or create the Redis client for release operations.

        This client uses the release connection pool, which is kept separate
        from acquire operations to prevent deadlocks.

        Returns:
            Redis client for release operations.
        """
        if self._release_client is None:
            self._release_client = Redis.from_pool(self._get_release_pool())
        return self._release_client

    async def reset(self):
        """Close all clients and connection pools, and reset internal state.

        This method should be called when the manager is no longer needed or
        before reusing it in a new context. It closes all Redis connections
        and resets internal state.
        """
        await self.get_acquire_client().aclose(close_connection_pool=True)
        await self.get_release_client().aclose(close_connection_pool=True)
        self._acquire_pool = None
        self._release_pool = None
        self._acquire_client = None
        self._release_client = None
