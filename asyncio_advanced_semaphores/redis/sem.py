import asyncio
import itertools
import logging
import math
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

import stlog
import tenacity
from tenacity import before_sleep_log

from asyncio_advanced_semaphores.acquisition_id import (
    _new_acquisition_id,
    _pop_acquisition_id,
    _push_acquisition_id,
    _remove_acquisition_id_any_task,
)
from asyncio_advanced_semaphores.common import (
    Semaphore,
    SemaphoreAcquisition,
    SemaphoreAcquisitionStats,
)
from asyncio_advanced_semaphores.redis import lua
from asyncio_advanced_semaphores.redis.client import (
    DEFAULT_REDIS_CLIENT_MANAGER,
    RedisClientManager,
)
from asyncio_advanced_semaphores.redis.key import (
    _extract_name_from_semaphore_key,
    _get_max_key,
    _get_semaphore_key,
    _get_semaphore_ttl_key,
)
from asyncio_advanced_semaphores.redis.ping import _PING_TASKS_MANAGER
from asyncio_advanced_semaphores.redis.ttl import (
    _start_ttl_checker_if_needed,
    _stop_and_drop_ttl_checkers,
)
from asyncio_advanced_semaphores.utils import super_shield

_MAX_REDIS_EXPIRE_VALUE = 2_147_483_647


class CantAcquireError(Exception):
    pass


@dataclass(kw_only=True, frozen=True)
class RedisSemaphore(Semaphore):
    heartbeat_max_interval: int = 10
    """The maximum interval (in seconds) between heartbeats. Only for Redis semaphore implementations.

    If a client doesn't send a heartbeat within the interval, the slot is released automatically.

    """
    client_manager: RedisClientManager = field(
        default_factory=lambda: DEFAULT_REDIS_CLIENT_MANAGER
    )
    _ping_interval: float = 1.0
    _max_wait_time: int = 1
    _start_to_poll_min_delay: float = 60.0
    _poll_delay: float = 1.0

    # Only for unit testing purposes
    _overriden_ping_func: Callable[[str], Coroutine[Any, Any, None]] | None = None

    def _get_semaphore_key(self, name: str) -> str:
        return _get_semaphore_key(
            name=name, namespace=self.client_manager.conf.namespace
        )

    def _get_semaphore_max_key(self, name: str) -> str:
        return _get_max_key(name=name, namespace=self.client_manager.conf.namespace)

    def _get_semaphore_ttl_key(self, name: str) -> str:
        return _get_semaphore_ttl_key(
            name=name, namespace=self.client_manager.conf.namespace
        )

    def _get_semaphore_waiting_key(self, name: str) -> str:
        return f"{self.client_manager.conf.namespace}:semaphore_waiting:{name}"

    def _get_acquisition_notification_key(self, acquisition_id: str) -> str:
        return f"{self.client_manager.conf.namespace}:acquisition_notification:{acquisition_id}"

    @property
    def _ttl(self) -> int:
        return (
            math.ceil(self.ttl)
            if self.ttl is not None
            else _MAX_REDIS_EXPIRE_VALUE - 10  # as we add 10 seconds in the LUA part
        )

    def _async_retrying(self) -> tenacity.AsyncRetrying:
        conf = self.client_manager.conf

        return tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(conf.number_of_attempts),
            wait=tenacity.wait_exponential(
                multiplier=conf.retry_multiplier,
                min=conf.retry_min_delay,
                max=conf.retry_max_delay,
            ),
            retry=tenacity.retry_if_not_exception_type(
                (CantAcquireError, asyncio.CancelledError, asyncio.TimeoutError)
            ),
            reraise=True,
            before_sleep=before_sleep_log(self._logger, logging.WARNING),  # type: ignore
        )

    async def _ping(self, acquisition_id: str) -> None:
        acquire_client = self.client_manager.get_acquire_client()
        ping_script = acquire_client.register_script(lua.PING_SCRIPT)
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                async with asyncio.timeout(self.heartbeat_max_interval / 2.0):
                    await ping_script(
                        keys=[
                            self._get_semaphore_waiting_key(self.name),
                            self._get_semaphore_key(self.name),
                        ],
                        args=[acquisition_id, self.heartbeat_max_interval, time.time()],
                    )
                    self._logger.debug("Acquisition key refreshed")
            except Exception:
                self._logger.warning("Error pinging => let's retry", exc_info=True)

    def _create_ping_task(self, acquisition_id: str) -> None:
        callback = (
            self._overriden_ping_func if self._overriden_ping_func else self._ping
        )
        _PING_TASKS_MANAGER.create_and_add_ping_task(
            acquisition_id=acquisition_id,
            callback=callback,
            semaphore_name=self.name,
        )

    async def acquire(self) -> SemaphoreAcquisition:
        before = time.perf_counter()
        _start_ttl_checker_if_needed(self.client_manager)
        acquisition_id = _new_acquisition_id()
        acquire_client = self.client_manager.get_acquire_client()
        queue_script = acquire_client.register_script(lua.QUEUE_SCRIPT)
        wake_up_nexts_script = acquire_client.register_script(lua.WAKE_UP_NEXTS_SCRIPT)
        acquire_script = acquire_client.register_script(lua.ACQUIRE_SCRIPT)
        with stlog.LogContext.bind(
            semaphore_name=self.name, acquisition_id=acquisition_id
        ):
            try:
                async with asyncio.timeout(self.max_acquire_time):
                    # Let's create the ping task to refresh the acquisition key
                    self._create_ping_task(acquisition_id)

                    async for attempt in self._async_retrying():
                        with attempt:
                            # Let's queue the acquisition
                            await queue_script(
                                keys=[
                                    self._get_semaphore_waiting_key(self.name),
                                ],
                                args=[
                                    acquisition_id,
                                    self.heartbeat_max_interval,
                                    self._ttl,
                                    time.time(),
                                ],
                            )
                            self._logger.debug("Acquisition queued")

                            # Let's wait for the acquisition to be notified
                            while True:
                                # Let's wake up the next acquisitions
                                await wake_up_nexts_script(
                                    keys=[
                                        self._get_semaphore_key(self.name),
                                        self._get_semaphore_waiting_key(self.name),
                                    ],
                                    args=[
                                        self.value,
                                        self.heartbeat_max_interval,
                                        self._ttl,
                                        time.time(),
                                        self._get_acquisition_notification_key(
                                            "@@@ACQUISITION_ID@@@"  # note: this will be replaced in the LUA part
                                        ),
                                    ],
                                )
                                # Let's wait for the acquisition to be notified
                                if (
                                    time.perf_counter() - before
                                    > self._start_to_poll_min_delay
                                ):
                                    # simple polling
                                    notified = await acquire_client.lpop(  # type: ignore
                                        self._get_acquisition_notification_key(
                                            acquisition_id
                                        ),
                                    )
                                    if notified is not None:
                                        break
                                    await asyncio.sleep(self._poll_delay)
                                else:
                                    # long polling (up to 1s)
                                    notified = await acquire_client.blpop(  # type: ignore
                                        [
                                            self._get_acquisition_notification_key(
                                                acquisition_id
                                            ),
                                        ],
                                        self._max_wait_time,
                                    )
                                    if notified is not None:
                                        break
                            self._logger.debug(
                                "Acquisition polling successful => let's acquire the slot..."
                            )

                            # Let's acquire the slot
                            changed = await acquire_script(
                                keys=[
                                    self._get_semaphore_key(self.name),
                                    self._get_semaphore_ttl_key(self.name),
                                    self._get_semaphore_max_key(self.name),
                                ],
                                args=[
                                    acquisition_id,
                                    self.value,
                                    self.heartbeat_max_interval,
                                    self._ttl,
                                    time.time(),
                                ],
                            )
                            if changed == 0:
                                raise CantAcquireError(
                                    "Acquisition failed, ZADD changed 0 elements => heartbeat/ttl issue ?"
                                )

            except BaseException:
                # note: catch any exception here (including asyncio.CancelledError)
                # we have to clean all acquired resources here
                # before re-raising the exception
                # This is very important to avoid leaking semaphores!
                self._logger.debug("Acquisition queueing failed => let's give up...")
                await self.__give_up_in_a_super_shield_task(acquisition_id)
                self._logger.debug("Acquisition given up => let's raise the exception")
                raise
            acquire_time = time.perf_counter() - before
            self._logger.info("Acquisition successful", acquire_time=acquire_time)
            _push_acquisition_id(self.name, acquisition_id)
            return SemaphoreAcquisition(id=acquisition_id, acquire_time=acquire_time)

    async def __give_up_in_a_super_shield_task(self, acquisition_id: str) -> None:
        task = asyncio.create_task(self.__release(acquisition_id))
        await super_shield(task, logger=self._logger)

    async def __release(self, acquisition_id: str) -> None:
        """Release logic, must be called with a shield to protected
        the code to be cancelled in the middle of the release."""
        _PING_TASKS_MANAGER.cancel_task(acquisition_id)
        release_client = self.client_manager.get_release_client()
        release_script = release_client.register_script(lua.RELEASE_SCRIPT)
        async for attempt in self._async_retrying():
            with attempt:
                released = await release_script(
                    keys=[
                        self._get_semaphore_key(self.name),
                        self._get_semaphore_ttl_key(self.name),
                        self._get_semaphore_waiting_key(self.name),
                    ],
                    args=[acquisition_id],
                )
        if released > 0:
            self._logger.debug("Acquisition released")

    async def arelease(self, acquisition_id: str | None = None) -> None:
        if acquisition_id is None:
            acquisition_id = _pop_acquisition_id(self.name)
            if acquisition_id is None:
                return
        else:
            # Remove the explicit acquisition_id from the tracker (may be from any task)
            _remove_acquisition_id_any_task(self.name, acquisition_id)
        await self.__give_up_in_a_super_shield_task(acquisition_id)

    def release(self, acquisition_id: str | None = None) -> None:
        raise NotImplementedError(
            "This method is not supported for RedisSemaphore => use arelease() coroutine instead"
        )

    def locked(self) -> bool:
        raise NotImplementedError(
            "This method is not supported for RedisSemaphore => use alocked() coroutine instead"
        )

    async def alocked(self) -> bool:
        acquire_client = self.client_manager.get_acquire_client()
        card_script = acquire_client.register_script(lua.CARD_SCRIPT)
        async for attempt in self._async_retrying():
            with attempt:
                card = await card_script(
                    keys=[self._get_semaphore_key(self.name)],
                    args=[time.time()],
                )
                return card >= self.value
        return False  # never reached, only for linters

    @classmethod
    async def get_acquisition_statistics(
        cls,
        *,
        names: list[str] | None = None,
        limit: int = 100,
        client_manager: RedisClientManager = DEFAULT_REDIS_CLIENT_MANAGER,
    ) -> dict[str, SemaphoreAcquisitionStats]:
        results: dict[str, SemaphoreAcquisitionStats] = {}
        client = client_manager.get_acquire_client()
        pattern = _get_semaphore_key(name="*", namespace=client_manager.conf.namespace)
        cursor: int | str = (
            "0"  # this is a hack to simplify the loop and indicate 0 withtout having to deal with the first iteration as an exception
        )
        while cursor != 0:
            async with client.pipeline(transaction=False) as pipe:
                keys: list[bytes] = []
                if names is None:
                    cursor, keys = await client.scan(
                        int(cursor), pattern, count=100, _type="ZSET"
                    )
                else:
                    keys = [
                        _get_semaphore_key(
                            name=name, namespace=client_manager.conf.namespace
                        ).encode()
                        for name in names
                    ]
                    cursor = 0  # let's break the loop after the first iteration
                for key in keys:
                    name = _extract_name_from_semaphore_key(key.decode())
                    max_key = _get_max_key(
                        name=name, namespace=client_manager.conf.namespace
                    )
                    now = time.time()
                    pipe.zremrangebyscore(key, "-inf", now)
                    pipe.get(max_key)
                    pipe.zcard(key)
                pipe_results = await pipe.execute()
            for i, batch in enumerate(itertools.batched(pipe_results, n=3)):
                key = keys[i]
                name = _extract_name_from_semaphore_key(key.decode())
                _, max_value, slots = batch
                if max_value is None:
                    continue
                max_value_int = int(max_value)
                results[name] = SemaphoreAcquisitionStats(
                    acquired_slots=slots, max_slots=max_value_int
                )
        sorted_results = sorted(
            results.items(), key=lambda x: x[1].acquired_percent, reverse=True
        )
        if names is not None:
            return dict(sorted_results)
        else:
            return dict(sorted_results[:limit])

    @classmethod
    async def stop_all(cls) -> None:
        await _stop_and_drop_ttl_checkers()
        await _PING_TASKS_MANAGER.clean()
