import asyncio
import itertools
import logging
import math
import threading
import time
import uuid
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

import stlog
import tenacity
from tenacity import before_sleep_log

from asyncio_advanced_semaphores.common import (
    _DEFAULT_LOGGER,
    AcquireResult,
    Semaphore,
    SemaphoreStats,
)
from asyncio_advanced_semaphores.redis import lua
from asyncio_advanced_semaphores.redis.client import (
    RedisClientManager,
    _get_client_manager,
)
from asyncio_advanced_semaphores.redis.conf import RedisConfig


def _extract_name_from_semaphore_key(key: str) -> str:
    return key.split(":")[-1]


def _get_semaphore_key(name: str, namespace: str) -> str:
    return f"{namespace}:semaphore_main:{name}"


def _get_semaphore_ttl_key(name: str, namespace: str) -> str:
    return f"{namespace}:semaphore_ttl:{name}"


def _get_max_key(name: str, namespace: str) -> str:
    return f"{namespace}:semaphore_max:{name}"


_MAX_REDIS_EXPIRE_VALUE = 2_147_483_647


class CantAcquireError(Exception):
    pass


async def _super_shield(
    task: asyncio.Task,
    timeout: float = 10.0,
    logger: logging.LoggerAdapter = _DEFAULT_LOGGER,
) -> None:
    """Completely shield the given task against cancellations.

    Waits for the task to complete up to the given timeout and completely
    ignores any cancellation errors in this interval.

    Args:
        task: The asyncio Task to shield.
        timeout: Maximum time to wait for task completion.
        logger: Logger for timeout warnings.
    """

    def timeout_warning() -> None:
        logger.warning(
            "super_shield: failed to end the shielded task within the timeout"
        )

    deadline = time.perf_counter() + timeout
    while True:
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            timeout_warning()
            break
        try:
            async with asyncio.timeout(remaining):
                await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            # Ignore cancellation and retry with remaining time
            continue
        except TimeoutError:
            timeout_warning()
            break


@dataclass(kw_only=True)
class RedisSemaphore(Semaphore):
    config: RedisConfig = field(default_factory=RedisConfig)
    """Redis configuration."""

    heartbeat_max_interval: int | None = 180
    """The maximum interval (in seconds) between two successful heartbeats.

    We recommend to NOT set this value too low (a few minutes is probably a good value).

    If a client doesn't send a successful heartbeat within the interval, the slot is released automatically.

    If set to None, the heartbeat is disabled (we discourage this setting).

    """

    ping_interval: float | None = None
    """ADVANCED: the interval (in seconds) between heartbeats.

    If set to None, we will calculate and use a good default value
    (heartbeat_max_interval divided by 5).

    Must be lower than heartbeat_max_interval.

    We recommend to let this value to None.

    """

    use_polling_after_delay: float = 100.0
    """Use polling (instead of BLPOP) after an acquisition wait longer that the provided delay (in seconds).

    This is useful to avoid using too much redis connections at the same time.

    """

    poll_delay: float = 1.0
    """The delay (in seconds) between polling attempts.
    
    Note: the polling is not used if the wait is less than use_polling_after_delay.
    
    """

    _blpop_max_wait_time: int = 1

    # Only for unit testing purposes
    _overriden_ping_func: Callable[[str], Coroutine[Any, Any, None]] | None = None

    # Internal storage for ping tasks (acquisition_id -> task)
    _ping_tasks: dict[str, asyncio.Task] = field(default_factory=dict)

    _ping_task_lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        if (
            self.heartbeat_max_interval is not None
            and self.ping_interval is not None
            and self.ping_interval >= self.heartbeat_max_interval
        ):
            raise ValueError("ping_interval must be lower than heartbeat_max_interval")
        super().__post_init__()

    def _get_type(self) -> str:
        return "redis"

    @property
    def _ping_interval(self) -> float:
        if self.ping_interval is not None:
            return self.ping_interval
        if self.heartbeat_max_interval is None:
            return 1.0  # won't be used, only for keeping the interface consistent
        return self.heartbeat_max_interval / 5.0

    @property
    def _heartbeat_max_interval(self) -> int:
        if self.heartbeat_max_interval is None:
            return _MAX_REDIS_EXPIRE_VALUE - 10  # as we add 10 seconds in the LUA part
        return self.heartbeat_max_interval

    @property
    def _client_manager(self) -> RedisClientManager:
        return _get_client_manager(self.config)

    def _get_semaphore_key(self, name: str) -> str:
        return _get_semaphore_key(name=name, namespace=self.config.namespace)

    def _get_semaphore_max_key(self, name: str) -> str:
        return _get_max_key(name=name, namespace=self.config.namespace)

    def _get_semaphore_ttl_key(self, name: str) -> str:
        return _get_semaphore_ttl_key(name=name, namespace=self.config.namespace)

    def _get_semaphore_waiting_key(self, name: str) -> str:
        return f"{self._client_manager.conf.namespace}:semaphore_waiting:{name}"

    def _get_semaphore_waiting_heartbeat_key(self, name: str) -> str:
        return (
            f"{self._client_manager.conf.namespace}:semaphore_waiting_heartbeat:{name}"
        )

    def _get_acquisition_notification_key(self, acquisition_id: str) -> str:
        return f"{self._client_manager.conf.namespace}:acquisition_notification:{acquisition_id}"

    @property
    def _resolved_ttl(self) -> int:
        return (
            math.ceil(self.ttl)
            if self.ttl is not None
            else _MAX_REDIS_EXPIRE_VALUE - 10  # as we add 10 seconds in the LUA part
        )

    def _async_retrying(self) -> tenacity.AsyncRetrying:
        conf = self._client_manager.conf

        return tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(conf.number_of_attempts),
            wait=tenacity.wait_exponential(
                multiplier=conf.retry_multiplier,
                min=conf.retry_min_delay,
                max=conf.retry_max_delay,
            ),
            retry=tenacity.retry_if_not_exception_type(
                (CantAcquireError, asyncio.CancelledError, TimeoutError)
            ),
            reraise=True,
            before_sleep=before_sleep_log(self._logger, logging.WARNING),  # type: ignore
        )

    async def _ping(self, acquisition_id: str) -> None:
        assert self.heartbeat_max_interval is not None
        acquire_client = self._client_manager.get_acquire_client()
        ping_script = acquire_client.register_script(lua.PING_SCRIPT)
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                async with asyncio.timeout(self.heartbeat_max_interval / 2.0):
                    await ping_script(
                        keys=[
                            self._get_semaphore_waiting_heartbeat_key(self.name),
                            self._get_semaphore_key(self.name),
                        ],
                        args=[acquisition_id, self.heartbeat_max_interval, time.time()],
                    )
                    self._logger.debug("Acquisition key refreshed")
            except Exception:
                self._logger.warning("Error pinging => let's retry", exc_info=True)

    def _pop_ping_task(self, acquisition_id: str) -> asyncio.Task | None:
        with self._ping_task_lock:
            return self._ping_tasks.pop(acquisition_id, None)

    def _create_ping_task(self, acquisition_id: str) -> None:
        callback = (
            self._overriden_ping_func if self._overriden_ping_func else self._ping
        )
        task = asyncio.create_task(callback(acquisition_id))
        with self._ping_task_lock:
            self._ping_tasks[acquisition_id] = task
        task.add_done_callback(lambda _: self._pop_ping_task(acquisition_id))

    async def _acquire(self) -> AcquireResult:
        before = time.perf_counter()
        acquisition_id = uuid.uuid4().hex
        acquire_client = self._client_manager.get_acquire_client()
        queue_script = acquire_client.register_script(lua.QUEUE_SCRIPT)
        wake_up_nexts_script = acquire_client.register_script(lua.WAKE_UP_NEXTS_SCRIPT)
        acquire_script = acquire_client.register_script(lua.ACQUIRE_SCRIPT)
        with stlog.LogContext.bind(
            semaphore_name=self.name, acquisition_id=acquisition_id
        ):
            try:
                async with asyncio.timeout(self.max_acquire_time):
                    if self.heartbeat_max_interval is not None:
                        # Let's create the ping task to refresh the acquisition key
                        self._create_ping_task(acquisition_id)

                    async for attempt in self._async_retrying():
                        with attempt:
                            # Let's queue the acquisition
                            await queue_script(
                                keys=[
                                    self._get_semaphore_waiting_key(self.name),
                                    self._get_semaphore_waiting_heartbeat_key(
                                        self.name
                                    ),
                                ],
                                args=[
                                    acquisition_id,
                                    self._heartbeat_max_interval,
                                    self._resolved_ttl,
                                    time.time(),
                                ],
                            )
                            self._logger.debug("Acquisition queued")
                            before = time.perf_counter()
                            # Let's wait for the acquisition to be notified
                            while True:
                                # Let's wake up the next acquisitions
                                await wake_up_nexts_script(
                                    keys=[
                                        self._get_semaphore_key(self.name),
                                        self._get_semaphore_ttl_key(self.name),
                                        self._get_semaphore_waiting_key(self.name),
                                        self._get_semaphore_waiting_heartbeat_key(
                                            self.name
                                        ),
                                    ],
                                    args=[
                                        self.value,
                                        self._heartbeat_max_interval,
                                        self._resolved_ttl,
                                        time.time(),
                                        self._get_acquisition_notification_key(
                                            "@@@ACQUISITION_ID@@@"  # note: this will be replaced in the LUA part
                                        ),
                                    ],
                                )
                                # Let's wait for the acquisition to be notified
                                if (
                                    time.perf_counter() - before
                                    > self.use_polling_after_delay
                                ):
                                    # simple polling (less redis connections used)
                                    notified = await acquire_client.lpop(  # type: ignore[invalid-await]
                                        self._get_acquisition_notification_key(
                                            acquisition_id
                                        ),
                                    )
                                    if notified is not None:
                                        break
                                    await asyncio.sleep(self.poll_delay)
                                else:
                                    # long polling (up to 1s, more reactive but more redis connections used)
                                    notified = await acquire_client.blpop(  # type: ignore[invalid-await]
                                        [
                                            self._get_acquisition_notification_key(
                                                acquisition_id
                                            ),
                                        ],
                                        self._blpop_max_wait_time,
                                    )
                                    if notified is not None:
                                        break
                            self._logger.debug(
                                "Acquisition polling successful => let's acquire the slot..."
                            )

                            # Let's acquire the slot
                            changed, card = await acquire_script(
                                keys=[
                                    self._get_semaphore_key(self.name),
                                    self._get_semaphore_ttl_key(self.name),
                                    self._get_semaphore_max_key(self.name),
                                ],
                                args=[
                                    acquisition_id,
                                    self.value,
                                    self._heartbeat_max_interval,
                                    self._resolved_ttl,
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
            return AcquireResult(acquisition_id=acquisition_id, slot_number=card)

    async def __give_up_in_a_super_shield_task(self, acquisition_id: str) -> None:
        task = asyncio.create_task(self.__release(acquisition_id))
        await _super_shield(task, logger=self._logger)

    def _cancel_ping_task(self, acquisition_id: str) -> None:
        task = self._pop_ping_task(acquisition_id)
        if task:
            task.cancel()

    async def __release(self, acquisition_id: str) -> None:
        """Release logic, must be called with a shield to protected
        the code to be cancelled in the middle of the release."""
        if self.heartbeat_max_interval is not None:
            self._cancel_ping_task(acquisition_id)
        release_client = self._client_manager.get_release_client()
        release_script = release_client.register_script(lua.RELEASE_SCRIPT)
        async for attempt in self._async_retrying():
            with attempt:
                await release_script(
                    keys=[
                        self._get_semaphore_key(self.name),
                        self._get_semaphore_ttl_key(self.name),
                        self._get_semaphore_waiting_key(self.name),
                        self._get_semaphore_waiting_heartbeat_key(self.name),
                    ],
                    args=[acquisition_id],
                )

    async def _release(self, acquisition_id: str) -> None:
        await self.__give_up_in_a_super_shield_task(acquisition_id)

    async def locked(self) -> bool:
        acquire_client = self._client_manager.get_acquire_client()
        card_script = acquire_client.register_script(lua.CARD_SCRIPT)
        async for attempt in self._async_retrying():
            with attempt:
                card = await card_script(
                    keys=[
                        self._get_semaphore_key(self.name),
                        self._get_semaphore_ttl_key(self.name),
                    ],
                    args=[time.time()],
                )
                return card >= self.value
        return False  # never reached, only for linters

    @classmethod
    async def get_acquired_stats(
        cls,
        *,
        names: list[str] | None = None,
        limit: int = 100,
        config: RedisConfig | None = None,
    ) -> dict[str, SemaphoreStats]:
        client_manager = _get_client_manager(config or RedisConfig())
        results: dict[str, SemaphoreStats] = {}
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
                results[name] = SemaphoreStats(
                    acquired_slots=slots, max_slots=max_value_int
                )
        sorted_results = sorted(
            results.items(), key=lambda x: x[1].acquired_percent, reverse=True
        )
        if names is not None:
            return dict(sorted_results)
        else:
            return dict(sorted_results[:limit])
