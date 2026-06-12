import asyncio
import contextlib
import inspect
import logging
from collections import deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

LOGGER = logging.getLogger(__name__)


class TaskLevel(IntEnum):
    """Level for queued task. Higher values dominate lower values."""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class QueuedTask:
    """A task waiting to be dequeued and executed by a consumer."""

    level: TaskLevel = field(
        metadata={"description": " Task level. Higher values dominate lower values."}
    )
    name: str = field (
        metadata={"description": "Task name for logging and debugging."}
    )
    coroutine: Coroutine[Any, Any, Any] = field(
        metadata={"description": "Coroutine to run when scheduled by a consumer."}
    )
    can_interrupt_running: bool = field(
        default=False,
        metadata={"description": "If True and level is strictly greater than the running level when, this task in the preempt slot instead of the deques."}
    )
    timeout: float = field(
        default=120,
        metadata={"description": "Timeout for the coroutine in seconds."}
    )


class LevelFilteredTaskQueue:
    """Sequential asyncio task queue with level-based filtering.

    This class is intended for use on a single event loop from coroutines only.
    It is not thread-safe across OS threads.
    """

    def __init__(
        self,
        on_queue_idle: Callable[..., Any] | None = None,
    ):
        """A class that enqueues tasks and processes them sequentially.
        
        :param on_queue_idle: sync or async callback invoked with no arguments when the queue drains
                            eg. For manual turn taking, set user turn or for auto turn taking, enable user mic
                            It can bind arguments ahead of time with functools.partial,
                            e.g. on_queue_idle=partial(callback_function, argument1, argument2, ...).
                            Async callbacks are awaited before the processor waits for more work.
                            If work arrives while an async idle callback is running, the callback is cancelled.
        """
        self._queue: deque[QueuedTask] = deque()

        # Callback to invoke when the queue drains
        self._on_queue_idle = on_queue_idle
        # Current running task
        self._running_task: QueuedTask | None = None
        # Current execution of asyncio.Task for current running task's coroutine
        self._current_execution: asyncio.Task | None = None
        # True while dequeuing or executing work; False while waiting on an empty queue
        self._is_queue_processing = False
        # Condition variable synchronizing queue access and processor wakeups
        self._cv = asyncio.Condition()
        # Incremented on each accepted submit; used to detect stale idle notifications
        self._activity_generation = 0
        self._processor_task: asyncio.Task | None = None
        self._idle_task: asyncio.Task | None = None

    def _ensure_processor_started_locked(self) -> None:
        """Start the long-lived processor. Caller must hold ``_cv``."""
        if self._processor_task is None or self._processor_task.done():
            self._processor_task = asyncio.create_task(self._run_processor())

    def _cancel_idle_task_locked(self) -> None:
        """Cancel an in-flight idle callback. Caller must hold ``_cv``."""
        if self._idle_task is not None and not self._idle_task.done():
            self._idle_task.cancel()

    def _highest_active_level_locked(self) -> TaskLevel | None:
        """Return the highest active level. Caller must hold ``_cv``."""
        max_level = None
        if self._running_task:
            max_level = self._running_task.level
        for queued_task in self._queue:
            if max_level is None or queued_task.level > max_level:
                max_level = queued_task.level
        return max_level

    async def get_highest_active_level(self) -> TaskLevel | None:
        """Get the highest active level currently present in the system (running or queued)."""
        async with self._cv:
            return self._highest_active_level_locked()

    async def is_queue_processing(self) -> bool:
        """Return True while dequeuing or executing work; False while waiting on an empty queue."""
        async with self._cv:
            return self._is_queue_processing

    async def get_running_task(self) -> QueuedTask | None:
        """Return the task currently executing, if any."""
        async with self._cv:
            return self._running_task

    async def queued_task_count(self) -> int:
        """Return the number of tasks waiting in the queue."""
        async with self._cv:
            return len(self._queue)

    async def submit_task(self, task: QueuedTask) -> bool:
        """Enqueue a incomming task.
        :param task: incoming task to be enqueued.
        :return: True if accepted; False if rejected.
        """
        async with self._cv:
            self._ensure_processor_started_locked()
            highest_active_level = self._highest_active_level_locked()
            # Reject incomming task if a strictly higher-level task is already running or queued 
            if highest_active_level is not None and task.level < highest_active_level:
                LOGGER.warning(
                        "Rejecting incoming task %s (level=%s) because its level is lower than the highest active level %s",
                        task.name,
                        task.level.name,
                        highest_active_level.name,
                    )
                return False

            # Evict all tasks from the back that have a strictly lower level than incoming task
            while self._queue and self._queue[-1].level < task.level:
                evicted = self._queue.pop()
                LOGGER.debug(
                    "Evicted queued task %s (level=%s) from queue because it is lower than the incoming task %s (level=%s).",
                    evicted.name,
                    evicted.level.name,
                    task.name,
                    task.level.name,
                )


            # Allow tasks to be enqueued
            self._queue.append(task)
            self._activity_generation += 1
            self._cancel_idle_task_locked()

            # Check whether there is a current execution of asyncio.Task for current running task's coroutine
            if self._current_execution and not self._current_execution.done():
                # Atomically inspect the next task in queue
                next_task_in_queue = self._queue[0]
                running_task = self._running_task
                # Preemption is triggered if the next task in queue outranks the currently running task and is flagged for preemption
                if (
                    running_task is not None
                    and next_task_in_queue.level > running_task.level
                    and next_task_in_queue.can_interrupt_running
                ):
                    LOGGER.debug(
                        "Task %s (level=%s) in queue with can_interrupt_running=True interrupt running task %s (level=%s)",
                        task.name,
                        task.level.name,
                        running_task.name,
                        running_task.level.name,
                    )

                    # Cancel running coroutine (triggers CancelledError)
                    self._current_execution.cancel()

            self._cv.notify()
            return True

    async def _run_processor(self) -> None:
        """Restart the processor automatically if it crashes unexpectedly."""
        while True:
            try:
                await self._process_queue()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Queue processor failed unexpectedly; restarting")
                await asyncio.sleep(0)
                async with self._cv:
                    if self._queue:
                        self._cv.notify()

    async def _process_queue(self):
        """Long-lived processor that waits for work and runs tasks one by one."""
        idle_callback = self._on_queue_idle

        while True:
            async with self._cv:
                while not self._queue:
                    self._running_task = None
                    self._current_execution = None
                    self._is_queue_processing = False
                    await self._cv.wait()

                self._is_queue_processing = True
                task = self._queue.popleft()
                self._running_task = task
                execution = asyncio.create_task(task.coroutine)
                self._current_execution = execution

            try:
                await asyncio.wait_for(
                    asyncio.shield(execution), timeout=task.timeout
                )
                LOGGER.info(
                    "Finished: '%s' (Level %s)",
                    task.name,
                    task.level.name,
                )
            except asyncio.TimeoutError:
                LOGGER.warning(
                    "Timeout: '%s' (Level %s)",
                    task.name,
                    task.level.name,
                )
                execution.cancel()
            except asyncio.CancelledError:
                LOGGER.info(
                    "Aborted: '%s' (Level %s)",
                    task.name,
                    task.level.name,
                )
            except Exception as e:
                LOGGER.warning(
                    "Failed with Error: '%s' (Level %s): %s",
                    task.name,
                    task.level.name,
                    e,
                )
            finally:
                await self._await_execution_finished(execution)

            async with self._cv:
                if not self._queue:
                    self._running_task = None
                    self._current_execution = None
                    self._is_queue_processing = False

            if callable(idle_callback):
                await self._invoke_idle_if_still_idle(idle_callback)

    @staticmethod
    async def _await_execution_finished(execution: asyncio.Task[Any]) -> None:
        """Wait for a task execution to fully finish, including cancellation cleanup."""
        if execution.done():
            with contextlib.suppress(asyncio.CancelledError):
                await execution
            return
        with contextlib.suppress(asyncio.CancelledError):
            await execution

    async def _invoke_idle_if_still_idle(self, idle_callback: Callable[..., Any]) -> None:
        """Invoke idle callback only if no work arrived while draining finished."""
        async with self._cv:
            if self._queue or self._is_queue_processing:
                return
            idle_generation = self._activity_generation

        # Let submit_task waiters that were blocked on the condition run before we notify idle.
        await asyncio.sleep(0)

        async with self._cv:
            still_idle = (
                not self._queue
                and not self._is_queue_processing
                and self._activity_generation == idle_generation
            )
            if not still_idle:
                return
            idle_task = asyncio.create_task(self._execute_idle_callback(idle_callback))
            self._idle_task = idle_task

        try:
            await idle_task
        except asyncio.CancelledError:
            LOGGER.debug("Idle callback cancelled because new work arrived")
        finally:
            async with self._cv:
                if self._idle_task is idle_task:
                    self._idle_task = None

    async def _execute_idle_callback(self, idle_callback: Callable[..., Any]) -> None:
        result = idle_callback()
        if inspect.isawaitable(result):
            await result
