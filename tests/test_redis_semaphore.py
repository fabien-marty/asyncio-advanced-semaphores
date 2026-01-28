import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from asyncio_advanced_semaphores import RedisSemaphore
from tests.common import get_new_redis_client_manager


async def test_redis_client_manager():
    redis_client_manager = get_new_redis_client_manager()
    await redis_client_manager.reset()


async def test_with_bad_heartbeat():
    async def bad_ping(_: str) -> None:
        return

    sem1 = RedisSemaphore(
        name="foo",
        value=1,
        ttl=60,
        heartbeat_max_interval=1,
        _overriden_ping_func=bad_ping,
    )
    sem2 = RedisSemaphore(
        name="foo",
        value=1,
        ttl=60,
    )

    result1 = (
        await sem1.acquire()
    )  # it will expire after 1 second (because no heartbeat)
    before = time.perf_counter()
    result2 = await sem2.acquire()
    after = time.perf_counter()
    assert after - before > 0.9
    await sem2.release(result2.acquisition_id)
    await sem1.release(result1.acquisition_id)


async def test_thread_safety_multiple_threads_same_semaphore():
    """Test thread-safety with multiple threads acquiring the same semaphore name.

    Each thread runs its own event loop and tries to acquire the same
    semaphore. The semaphore has value=1, so only one thread can hold it
    at a time. We verify proper synchronization by tracking concurrent
    acquisitions.
    """
    sem_name = f"thread-safe-test-{time.time()}"
    sem_value = 1
    acquisitions_per_thread = 20
    number_of_threads = 5

    # Shared state protected by a lock
    lock = threading.Lock()
    concurrent_count = 0
    max_concurrent = 0
    errors: list[str] = []

    def thread_worker() -> None:
        """Run async semaphore acquisitions in a new event loop."""

        async def acquire_multiple_times() -> None:
            nonlocal concurrent_count, max_concurrent
            sem = RedisSemaphore(
                name=sem_name,
                value=sem_value,
                max_acquire_time=30,
            )

            for _ in range(acquisitions_per_thread):
                async with sem.cm():
                    with lock:
                        concurrent_count += 1
                        max_concurrent = max(max_concurrent, concurrent_count)
                        if concurrent_count > sem_value:
                            errors.append(
                                f"Concurrent limit exceeded: {concurrent_count} > {sem_value}"
                            )
                    # Hold the semaphore briefly
                    await asyncio.sleep(0.01)
                    with lock:
                        concurrent_count -= 1

        asyncio.run(acquire_multiple_times())

    # Run threads concurrently
    with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
        futures = [executor.submit(thread_worker) for _ in range(number_of_threads)]
        for future in futures:
            future.result()  # Re-raise any exceptions

    # Verify no errors occurred
    assert not errors, f"Thread-safety errors: {errors}"
    # Verify semaphore limit was respected
    assert max_concurrent <= sem_value, f"Max concurrent {max_concurrent} > {sem_value}"
    # Verify the semaphore was actually contested (at least once both threads were active)
    assert max_concurrent == sem_value, "Semaphore was never fully utilized"


async def test_waiting_longer_than_ttl_plus_10():
    max_acquire_time = 0.0

    async def worker(sem: RedisSemaphore) -> None:
        nonlocal max_acquire_time
        before = time.perf_counter()
        async with sem.cm() as result:
            after = time.perf_counter()
            max_acquire_time = max(max_acquire_time, after - before)
            print(f"Acquired: {result.acquisition_id} in {after - before} seconds")  # noqa: T201
            await asyncio.sleep(4)

    sem = RedisSemaphore(
        name="fabien",
        value=1,
        ttl=5,
    )
    await asyncio.gather(
        worker(sem), worker(sem), worker(sem), worker(sem), worker(sem)
    )
    assert max_acquire_time > 16
