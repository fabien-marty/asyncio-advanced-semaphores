import asyncio
import logging
import time

import stlog

_DEFAULT_LOGGER = stlog.getLogger("asyncio_advanced_semaphores")


def _get_current_task() -> asyncio.Task:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task(loop)
    if task is None:
        raise Exception("No running asyncio task found")  # pragma: no cover
    return task


async def super_shield(
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
