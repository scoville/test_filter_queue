import asyncio

import pytest
from task_queue import FilterQueue, FilterQueueConsumer, QueuedTask, TaskLevel

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")

# Required Behavior 1: Only one task runs at a time
@pytest.mark.asyncio
async def test_only_one_task_runs_at_a_time() -> None:
    queue = FilterQueue()
    consumer = FilterQueueConsumer(queue)
    consumer.start_worker()


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

    task_a = QueuedTask(level=TaskLevel.NORMAL, name="a", coroutine=work("a", 0.05))
    task_b = QueuedTask(level=TaskLevel.NORMAL, name="b", coroutine=work("b", 0.05))

    assert queue.submit_task(task_a)
    assert queue.submit_task(task_b)
    await asyncio.sleep(0.2)
    assert max_active == 1
    consumer.stop_worker()

# Required Behavior 2a: Higher level task from queue do not interrupt running task if can_interrupt_running is False
@pytest.mark.asyncio
async def test_higher_level_does_not_interrupt_running_task_with_can_interrupt_running_false() -> None:
    queue = FilterQueue()
    consumer = FilterQueueConsumer(queue)
    consumer.start_worker()


    low_cancelled = asyncio.Event()
    order: list[str] = []

    async def low() -> None:
        try:
            await asyncio.sleep(0.2)
            order.append("low_done")
        except asyncio.CancelledError:
            low_cancelled.set()
            raise

    async def high() -> None:
        await asyncio.sleep(0)
        order.append("high_done")

    task_low = QueuedTask(level=TaskLevel.LOW, name="low", coroutine=low())
    task_high = QueuedTask(level=TaskLevel.HIGH, name="high", coroutine=high())

    # long sleep in low task, so it has been running when high task are submitted
    assert queue.submit_task(task_low)
    await asyncio.sleep(0.02)
    assert queue.submit_task(task_high)
    await asyncio.sleep(0.35)
    assert not low_cancelled.is_set()
    assert order == ["low_done", "high_done"]
    consumer.stop_worker()

# Required Behavior 2b: Higher level task from queue interrupt running task if can_interrupt_running is True
@pytest.mark.asyncio
async def test_higher_level_interrupt_running_task_with_can_interrupt_running_true() -> None:
    queue = FilterQueue()
    consumer = FilterQueueConsumer(queue)
    consumer.start_worker()

    low_cancelled = asyncio.Event()
    order: list[str] = []

    async def low() -> None:
        try:
            await asyncio.sleep(0.2)
            order.append("low_done")
        except asyncio.CancelledError:
            low_cancelled.set()
            raise

    async def high() -> None:
        await asyncio.sleep(0)
        order.append("high_done")

    task_low = QueuedTask(level=TaskLevel.LOW, name="low", coroutine=low())
    task_high = QueuedTask(level=TaskLevel.HIGH, name="high", coroutine=high(), can_interrupt_running=True)

    # long sleep in low task, so it has been running when high task are submitted
    assert queue.submit_task(task_low)
    await asyncio.sleep(0.02)
    assert queue.submit_task(task_high)
    await asyncio.sleep(0.35)
    assert low_cancelled.is_set()
    assert order == ["high_done"]
    consumer.start_worker()

# Required Behavior 2c: Lower level task cannot interrupt running task even though can_interrupt_running is True
@pytest.mark.asyncio
async def  test_higher_level_does_not_interrupt_running_task_with_can_interrupt_running_true() -> None:
    queue = FilterQueue()
    consumer = FilterQueueConsumer(queue)
    consumer.start_worker()

    high_cancelled = asyncio.Event()
    order: list[str] = []

    async def high() -> None:
        try:
            await asyncio.sleep(0.2)
            order.append("high_done")
        except asyncio.CancelledError:
            high_cancelled.set()
            raise

    async def low() -> None:
        await asyncio.sleep(0)
        order.append("low_done")

    task_high = QueuedTask(level=TaskLevel.HIGH, name="high", coroutine=high())
    task_low = QueuedTask(level=TaskLevel.LOW, name="low", coroutine=low(), can_interrupt_running=True)

    # long sleep in high task, so it has been running when low task are submitted
    assert queue.submit_task(task_high)
    await asyncio.sleep(0.02)
    assert queue.submit_task(task_low)
    await asyncio.sleep(0.35)
    assert not high_cancelled.is_set()
    assert order == ["high_done", "low_done"]
    consumer.stop_worker()

# Required Behavior 3: Higher-level incoming tasks evict lower-level tasks from queue
@pytest.mark.asyncio
async def test_higher_level_incoming_tasks_evicts_lower_queued() -> None:
    queue = FilterQueue()
    consumer = FilterQueueConsumer(queue)
    consumer.start_worker()

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

    task_low = QueuedTask(level=TaskLevel.LOW, name="low", coroutine=low())
    task_high = QueuedTask(level=TaskLevel.HIGH, name="high", coroutine=high())
    task_critical = QueuedTask(level=TaskLevel.CRITICAL, name="critical", coroutine=critical())

    # long sleep in low task, so it has beee running when high task and critical task are submitted
    assert queue.submit_task(task_low)
    await asyncio.sleep(0.02)
    # high task and critical task are in queue and thus high task is removed from queue
    assert queue.submit_task(task_high)
    await asyncio.sleep(0.02)
    assert queue.submit_task(task_critical)
    await asyncio.sleep(0.4)
    assert ran == ["low", "critical"]
    consumer.stop_worker()

# Required Behavior 4: Do not enqueue an incoming task if any task queued has strictly higher priority
@pytest.mark.asyncio
async def test_do_not_enqueue_incoming_task_when_higher_queued() -> None:
    queue = FilterQueue()
    consumer = FilterQueueConsumer(queue)
    consumer.start_worker()

    ran: list[str] = []

    async def blocker() -> None:
        ran.append("blocker")
        await asyncio.sleep(0.2)

    async def high_queued() -> None:
        await asyncio.sleep(0)
        ran.append("high_queued")

    async def rejected() -> None:
        await asyncio.sleep(0)
        ran.append("rejected")

    
    task_blocker = QueuedTask(level=TaskLevel.NORMAL, name="blocker", coroutine=blocker())
    task_high_queued = QueuedTask(level=TaskLevel.HIGH, name="high_queued", coroutine=high_queued())
    task_rejected = QueuedTask(level=TaskLevel.NORMAL, name="rejected", coroutine=rejected())

    # long sleep in broker task, so it has been running when high_queued task and rejected task are submitted
    assert queue.submit_task(task_blocker)
    await asyncio.sleep(0.02)
    assert  queue.submit_task(task_high_queued)
    await asyncio.sleep(0.02)
    # high_queued task is now in queue and try to submit rejected task
    accepted = queue.submit_task(task_rejected)
    # check whether rejected task is not enqueued
    assert not accepted
    await asyncio.sleep(0.35)
    assert ran == ["blocker", "high_queued"]
    consumer.stop_worker()

# Required Behavior 5: Interrupt running task if its predefined task timeout happens
@pytest.mark.asyncio
async def test_task_times_out_when_exceeding_limit() -> None:
    queue = FilterQueue()
    consumer = FilterQueueConsumer(queue)
    consumer.start_worker()

    ran: list[str] = []

    async def slow() -> None:
        await asyncio.sleep(1.0)
        ran.append("slow_done")

    async def fast() -> None:
        await asyncio.sleep(0)
        ran.append("fast_done")

    task_slow = QueuedTask(
        level=TaskLevel.NORMAL,
        name="slow",
        coroutine=slow(),
        timeout=0.05,
    )
    task_fast = QueuedTask(level=TaskLevel.NORMAL, name="fast", coroutine=fast())

    assert queue.submit_task(task_slow)
    await asyncio.sleep(0.15)
    assert "slow_done" not in ran

    assert queue.submit_task(task_fast)
    await asyncio.sleep(0.15)
    assert ran == ["fast_done"]
    consumer.stop_worker()
