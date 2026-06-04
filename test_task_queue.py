import asyncio
from collections.abc import Coroutine
from typing import Any

import pytest
from task_queue import AsyncTaskQueue, TaskPriority

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")

# Rule 1: Only one task runs at a time
@pytest.mark.asyncio
async def test_only_one_task_runs_at_a_time() -> None:
    queue = AsyncTaskQueue()
    active = 0
    max_active = 0
    lock = asyncio.Lock()

    async def work(_label: str, delay: float) -> None:
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(delay)
        async with lock:
            active -= 1

    assert await queue.submit_async(work("a", 0.05), priority=TaskPriority.NORMAL, name="a")
    assert await queue.submit_async(work("b", 0.05), priority=TaskPriority.NORMAL, name="b")
    await asyncio.sleep(0.2)
    await queue.cancel()
    assert max_active == 1

# Rule 2: Higher priority tasks not interrupt running tasks if can_interrupt_running is False
@pytest.mark.asyncio
async def test_higher_priority_does_not_cancel_running() -> None:
    queue = AsyncTaskQueue()
    cancelled = asyncio.Event()
    order: list[str] = []

    async def low() -> None:
        try:
            await asyncio.sleep(0.2)
            order.append("low_done")
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def high() -> None:
        await asyncio.sleep(0)
        order.append("high_done")

    # long sleep in low task, so it has beee running when high task are submitted
    assert await queue.submit_async(low(), priority=TaskPriority.LOW, name="low")
    await asyncio.sleep(0.02)
    assert await queue.submit_async(high(), priority=TaskPriority.HIGH, name="high")
    await asyncio.sleep(0.35)
    await queue.cancel()
    assert not cancelled.is_set()
    assert order == ["low_done", "high_done"]

# Rule 3: Higher priority tasks evict lower priority tasks
@pytest.mark.asyncio
async def test_higher_priority_evicts_lower_queued() -> None:
    queue = AsyncTaskQueue()
    ran: list[str] = []

    async def low() -> None:
        ran.append("low")
        await asyncio.sleep(0.15)

    async def high() -> None:
        ran.append("high")
        await asyncio.sleep(0.01)

    async def critical() -> None:
        await asyncio.sleep(0)
        ran.append("critical")

    # long sleep in low task, so it has beee running when high task and critical task are submitted
    assert await queue.submit_async(low(), priority=TaskPriority.LOW, name="low")
    await asyncio.sleep(0.02)
    # high task and critical task are in queue and thus high task is removed from queue
    assert await queue.submit_async(high(), priority=TaskPriority.HIGH, name="high")
    await asyncio.sleep(0.02)
    assert await queue.submit_async(critical(), priority=TaskPriority.CRITICAL, name="critical")
    await asyncio.sleep(0.4)
    await queue.cancel()
    assert ran == ["low", "critical"]

# Rule 4-5: FIFO within the same priority
@pytest.mark.asyncio
async def test_fifo_within_same_priority() -> None:
    queue = AsyncTaskQueue()
    order: list[str] = []

    async def work(label: str) -> None:
        order.append(label)
        await asyncio.sleep(0.01)

    assert await queue.submit_async(work("first"), priority=TaskPriority.NORMAL, name="first")
    assert await queue.submit_async(work("second"), priority=TaskPriority.NORMAL, name="second")
    await asyncio.sleep(0.1)
    await queue.cancel()
    assert order == ["first", "second"]


# Rule 1 (opt-in): Higher priority tasks interrupt running tasks if can_interrupt_running is True
@pytest.mark.asyncio
async def test_interrupt_requires_higher_priority_and_flag() -> None:
    queue = AsyncTaskQueue()
    low_cancelled = asyncio.Event()
    order: list[str] = []

    async def low() -> None:
        try:
            await asyncio.sleep(0.3)
            order.append("low_done")
        except asyncio.CancelledError:
            low_cancelled.set()
            raise

    async def high() -> None:
        await asyncio.sleep(0)
        order.append("high_done")

    assert await queue.submit_async(low(), priority=TaskPriority.LOW, name="low")
    await asyncio.sleep(0.02)
    assert await queue.submit_async(
        high(), priority=TaskPriority.HIGH, name="high", can_interrupt_running=True
    )
    await asyncio.sleep(0.15)
    await queue.cancel()
    assert low_cancelled.is_set()
    assert order == ["high_done"]

# Rule 1 (opt-in): Lower priority tasks cannot interrupt running tasks even though can_interrupt_running is True
@pytest.mark.asyncio
async def test_interrupt_flag_without_higher_priority_rejected() -> None:
    queue = AsyncTaskQueue()
    cancelled = asyncio.Event()
    high_finished = asyncio.Event()

    async def high() -> None:
        try:
            await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            high_finished.set()

    async def low() -> None:
        pass

    assert await queue.submit_async(high(), priority=TaskPriority.HIGH, name="high")
    await asyncio.sleep(0.02)
    accepted = await queue.submit_async(
        low(), priority=TaskPriority.LOW, name="low", can_interrupt_running=True
    )
    assert not accepted
    await asyncio.wait_for(high_finished.wait(), timeout=1.0)
    await queue.cancel()
    assert not cancelled.is_set()

# Rule 6: Do not enqueue an incoming task if any task currently running or queued has strictly higher priority
@pytest.mark.asyncio
async def test_rule6_rejects_lower_when_higher_queued() -> None:
    queue = AsyncTaskQueue()
    ran: list[str] = []

    async def blocker() -> None:
        ran.append("blocker")
        await asyncio.sleep(0.2)

    async def high_queued() -> None:
        await asyncio.sleep(0)
        ran.append("high_queued")

    assert await queue.submit_async(blocker(), priority=TaskPriority.NORMAL, name="blocker")
    await asyncio.sleep(0.02)
    assert await queue.submit_async(high_queued(), priority=TaskPriority.HIGH, name="high_queued")
    await asyncio.sleep(0.02)

    async def rejected() -> None:
        await asyncio.sleep(0)
        ran.append("rejected")

    rejected_coro = rejected()
    accepted = await queue.submit_async(
        rejected_coro, priority=TaskPriority.NORMAL, name="normal_rejected"
    )
    assert not accepted
    rejected_coro.close()

    await asyncio.sleep(0.35)
    await queue.cancel()
    assert ran == ["blocker", "high_queued"]

# Rule 6: Enqueue an incoming task if no task currently running or queued has strictly higher priority
@pytest.mark.asyncio
async def test_rule6_equal_priority_can_queue() -> None:
    queue = AsyncTaskQueue()
    order: list[str] = []

    async def first() -> None:
        order.append("first")
        await asyncio.sleep(0.05)

    async def second() -> None:
        await asyncio.sleep(0)
        order.append("second")

    assert await queue.submit_async(first(), priority=TaskPriority.NORMAL, name="first")
    await asyncio.sleep(0.01)
    assert await queue.submit_async(second(), priority=TaskPriority.NORMAL, name="second")
    await asyncio.sleep(0.15)
    await queue.cancel()
    assert order == ["first", "second"]

# Test rejected coroutines are closed
@pytest.mark.asyncio
async def test_rejected_coroutine_is_closed() -> None:
    queue = AsyncTaskQueue()
    closed_coroutines: list[Coroutine[Any, Any, Any]] = []
    original_close = queue._close_coroutine

    def track_close(coroutine: Coroutine[Any, Any, Any]) -> None:
        closed_coroutines.append(coroutine)
        original_close(coroutine)

    queue._close_coroutine = track_close  # type: ignore[method-assign]

    async def high() -> None:
        await asyncio.sleep(0.15)

    async def low() -> None:
        pass

    assert await queue.submit_async(high(), priority=TaskPriority.HIGH, name="high")
    await asyncio.sleep(0.02)
    low_coro = low()
    accepted = await queue.submit_async(low_coro, priority=TaskPriority.LOW, name="low")
    assert not accepted
    assert low_coro in closed_coroutines

    await asyncio.sleep(0.2)
    await queue.cancel()
