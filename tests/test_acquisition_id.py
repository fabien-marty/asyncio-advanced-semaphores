import asyncio

from asyncio_advanced_semaphores.acquisition_id import (
    _new_acquisition_id,
    _pop_acquisition_id,
    _push_acquisition_id,
)


async def test_new_acquisition_id():
    acquisition_id1 = _new_acquisition_id()
    acquisition_id2 = _new_acquisition_id()
    assert len(acquisition_id1) >= 32
    assert len(acquisition_id2) >= 32
    assert acquisition_id1 != acquisition_id2


async def test_push_and_pop_acquisition_id():
    acquisition_id1 = _new_acquisition_id()
    acquisition_id2 = _new_acquisition_id()
    _push_acquisition_id("test", acquisition_id1)
    _push_acquisition_id("test", acquisition_id2)
    assert _pop_acquisition_id("test") == acquisition_id2
    assert _pop_acquisition_id("test") == acquisition_id1
    assert _pop_acquisition_id("test") is None


async def test_task():
    async def task_function():
        acquisition_id3 = _new_acquisition_id()
        _push_acquisition_id("test", acquisition_id3)
        await asyncio.sleep(0.5)
        assert _pop_acquisition_id("test") == acquisition_id3

    acquisition_id1 = _new_acquisition_id()
    acquisition_id2 = _new_acquisition_id()
    _push_acquisition_id("test", acquisition_id1)
    task = asyncio.create_task(task_function())
    await asyncio.sleep(0.1)
    _push_acquisition_id("test", acquisition_id2)
    await task
    assert _pop_acquisition_id("test") == acquisition_id2
    assert _pop_acquisition_id("test") == acquisition_id1
    assert _pop_acquisition_id("test") is None
