import asyncio
import logging

import pytest

from asyncio_advanced_semaphores.redis.sem import _super_shield


async def test_super_shield_task_completes_normally():
    """Test that super_shield waits for a task that completes normally."""
    result = []

    async def simple_task():
        await asyncio.sleep(0.05)
        result.append("done")

    task = asyncio.create_task(simple_task())
    await _super_shield(task, timeout=1.0)

    assert result == ["done"]
    assert task.done()


async def test_super_shield_protects_against_cancellation():
    """Test that super_shield protects the task against external cancellation."""
    result = []

    async def slow_task():
        await asyncio.sleep(0.1)
        result.append("completed")

    task = asyncio.create_task(slow_task())

    async def cancel_after_delay():
        await asyncio.sleep(0.02)
        # This cancellation should be caught and ignored by super_shield
        raise asyncio.CancelledError()

    async def shielded_wrapper():
        await _super_shield(task, timeout=1.0)

    # Run super_shield in a task that will receive cancellation
    shield_task = asyncio.create_task(shielded_wrapper())

    # Cancel the shield_task while the inner task is running
    await asyncio.sleep(0.02)
    shield_task.cancel()

    # Wait for the original task to complete
    await asyncio.sleep(0.15)

    # The inner task should have completed despite the cancellation
    assert result == ["completed"]
    assert task.done()


async def test_super_shield_timeout_exceeded(caplog: pytest.LogCaptureFixture):
    """Test that super_shield logs warning when timeout is exceeded."""

    async def very_slow_task():
        await asyncio.sleep(10.0)  # Much longer than timeout

    task = asyncio.create_task(very_slow_task())

    with caplog.at_level(logging.WARNING):
        await _super_shield(task, timeout=0.1)

    assert (
        "super_shield: failed to end the shielded task within the timeout"
        in caplog.text
    )

    # Cleanup: cancel the slow task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_super_shield_multiple_cancellation_attempts():
    """Test that super_shield handles multiple cancellation attempts."""
    result = []
    cancel_count = 0

    async def task_with_work():
        await asyncio.sleep(0.15)
        result.append("done")

    task = asyncio.create_task(task_with_work())

    async def shield_with_cancellations():
        nonlocal cancel_count
        try:
            await _super_shield(task, timeout=1.0)
        except asyncio.CancelledError:
            cancel_count += 1
            raise

    shield_task = asyncio.create_task(shield_with_cancellations())

    # Send multiple cancellation attempts
    for _ in range(3):
        await asyncio.sleep(0.03)
        shield_task.cancel()

    # Wait for the inner task to complete
    await asyncio.sleep(0.2)

    # The inner task should complete despite multiple cancellations
    assert result == ["done"]
    assert task.done()


async def test_super_shield_with_already_completed_task():
    """Test super_shield with a task that completes immediately."""

    async def instant_task():
        return "instant"

    task = asyncio.create_task(instant_task())
    await asyncio.sleep(0)  # Let the task complete

    await _super_shield(task, timeout=1.0)

    assert task.done()
    assert task.result() == "instant"


async def test_super_shield_with_failing_task():
    """Test super_shield propagates exception from the inner task."""

    async def failing_task():
        await asyncio.sleep(0.01)
        raise ValueError("task failed")

    task = asyncio.create_task(failing_task())

    # super_shield should propagate the exception from the inner task
    with pytest.raises(ValueError, match="task failed"):
        await _super_shield(task, timeout=1.0)

    assert task.done()


async def test_super_shield_custom_logger(caplog: pytest.LogCaptureFixture):
    """Test that super_shield uses the provided custom logger."""
    base_logger = logging.getLogger("custom_test_logger")
    custom_logger = logging.LoggerAdapter(base_logger, {})

    async def very_slow_task():
        await asyncio.sleep(10.0)

    task = asyncio.create_task(very_slow_task())

    with caplog.at_level(logging.WARNING, logger="custom_test_logger"):
        await _super_shield(task, timeout=0.1, logger=custom_logger)

    assert (
        "super_shield: failed to end the shielded task within the timeout"
        in caplog.text
    )

    # Cleanup
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
