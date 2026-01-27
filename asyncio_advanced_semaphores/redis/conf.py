from dataclasses import dataclass


@dataclass(kw_only=True, frozen=True)
class RedisConfig:
    namespace: str = "adv-sem"
    """Namespace for redis keys."""

    url: str = "redis://localhost:6379"
    """Redis connection URL (e.g., "redis://localhost:6379")."""

    max_connections: int = 300
    """Redis maximum number of connections.
    
    Important: this number is per event loop. Be careful if you use several threads/event loops.
    """

    socket_timeout: int = 30
    """Redis timeout for socket operations (seconds)."""

    socket_connect_timeout: int = 10
    """Redis timeout for establishing socket connections (seconds)."""

    number_of_attempts: int = 3
    """Number of attempts to retry Redis operations."""

    retry_multiplier: float = 2
    """Multiplier for the delay between Redis operations (in case of failures/retries)."""

    retry_min_delay: float = 1
    """Minimum delay between Redis operations (seconds)."""

    retry_max_delay: float = 60
    """Maximum delay between Redis operations (seconds)."""
