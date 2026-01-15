import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field

import stlog

from asyncio_advanced_semaphores.redis import lua
from asyncio_advanced_semaphores.redis.client import RedisClientManager
from asyncio_advanced_semaphores.redis.conf import RedisConfig
from asyncio_advanced_semaphores.redis.key import (
    _get_semaphore_key,
    _get_semaphore_ttl_key,
)
from asyncio_advanced_semaphores.redis.ping import _PING_TASKS_MANAGER


@dataclass
class _TTLChecker:
    client_manager: RedisClientManager
    every: float = 1.0
    _task: asyncio.Task | None = None
    _check_timeout: float = 10.0
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    _logger: logging.LoggerAdapter = field(
        default_factory=lambda: stlog.getLogger("asyncio_advanced_semaphores")
    )

    def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self.check())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        await self._task
        self._task = None

    async def _check(self, semaphore_name: str) -> None:
        release_client = self.client_manager.get_release_client()
        clean_ttl_script = release_client.register_script(lua.CLEAN_TTL_SCRIPT)
        await clean_ttl_script(
            keys=[
                _get_semaphore_key(semaphore_name, self.client_manager.conf.namespace),
                _get_semaphore_ttl_key(
                    semaphore_name, self.client_manager.conf.namespace
                ),
            ],
            args=[time.time()],
        )

    async def check(self) -> None:
        while not self._stop_event.is_set():
            try:
                for semaphore_name in _PING_TASKS_MANAGER.get_semaphore_names():
                    async with asyncio.timeout(self._check_timeout):
                        await self._check(semaphore_name)
            except Exception:
                self._logger.warning(
                    "Error checking TTL => let's retry later", exc_info=True
                )
            try:
                async with asyncio.timeout(self.every):
                    await self._stop_event.wait()
            except TimeoutError:
                pass


_TTL_CHECKERS: dict[RedisConfig, _TTLChecker] = {}
_TTL_CHECKERS_LOCK = threading.Lock()


def _start_ttl_checker_if_needed(client_manager: RedisClientManager) -> None:
    with _TTL_CHECKERS_LOCK:
        if client_manager.conf not in _TTL_CHECKERS:
            ttl_checker = _TTLChecker(client_manager=client_manager)
            _TTL_CHECKERS[client_manager.conf] = ttl_checker
            ttl_checker.start()


async def _stop_and_drop_ttl_checkers() -> None:
    # Extract checkers and clear dict while holding the lock,
    # then await outside the lock to avoid blocking other operations
    with _TTL_CHECKERS_LOCK:
        checkers_to_stop = list(_TTL_CHECKERS.values())
        _TTL_CHECKERS.clear()
    for ttl_checker in checkers_to_stop:
        await ttl_checker.stop()
