import asyncio
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


class ConditionalPreemptiveScheduler:
    def __init__(
        self,
        on_queue_idle: Callable[..., Any] | None = None,
    ):
        """Scheduler class that enqueues tasks and processes them sequentially.
        
        :param on_queue_idle: callback function that is invoked with no arguments when the queue drains
                            eg. For manual turn taking, set user turn or for auto turn taking, enable user mic
                            It can Bind arguments ahead of time with functools.partial, 
                            e.g. on_queue_idle=partial(callback_function, argument1, argument2, ...).
        """
        self.queue: deque[QueuedTask] = deque()

        # Callback to invoke when the queue drains
        self._on_queue_idle = on_queue_idle
        # Current running task          
        self.running_task: QueuedTask | None = None
        # Current execution of asyncio.Task for current running task's coroutine
        self._current_execution: asyncio.Task | None = None
        # Flag to indicate whether the queue is currently being processed so that only one process_queue task is running at a time
        self.is_queue_processing = False
        # Lock to synchronize access to the queue and running task
        self._lock = asyncio.Lock()
 
    def get_highest_active_level(self) -> TaskLevel | None:
        """Get the highest active level currently present in the system (running or queued)."""
        max_level = None
        if self.running_task:
            max_level = self.running_task.level
        for queued_task in self.queue:
            if max_level is None or queued_task.level > max_level:
                max_level = queued_task.level
        return max_level

    async def submit_task(self, task: QueuedTask) -> bool:
        """Enqueue a incomming task.
        :param task: incoming task to be enqueued.
        :return: True if accepted; False if rejected.
        """
        async with self._lock:
            highest_active_level = self.get_highest_active_level()
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
            while self.queue and self.queue[-1].level < task.level:
                evicted = self.queue.pop()
                LOGGER.debug(
                    "Evicted queued task %s (level=%s) from queue because it is lower than the incoming task %s (level=%s).",
                    evicted.name,
                    evicted.level.name,
                    task.name,
                    task.level.name,
                )


            # Allow tasks to be enqueued
            self.queue.append(task)

            # Check whether there is a current execution of asyncio.Task for current running task's coroutine
            if self._current_execution and not self._current_execution.done():
                # Atomically inspect the next task in queue
                next_task_in_queue = self.queue[0]
                # Preemption is triggered if the next task in queue outranks the currently running task and is flagged for preemption
                if next_task_in_queue.level > self.running_task.level and next_task_in_queue.can_interrupt_running:
                    LOGGER.debug("Task %s (level=%s) in queue with can_interrupt_running=True interrupt running task %s (level=%s)", 
                    task.name, 
                    task.level.name, 
                    self.running_task.name, 
                    self.running_task.level.name)

                    # Cancel running coroutine (triggers CancelledError)
                    self._current_execution.cancel()

            # Start process queue only when queue is not being processed.
            if not self.is_queue_processing:
                self.is_queue_processing = True
                asyncio.create_task(self._process_queue())

            return True

    async def _process_queue(self):
        """Process queue to keep getting next task from queue and then run it one by one. """
        idle_callback = self._on_queue_idle

        # Process queue until the queue is empty
        while True:
            # Acquire the lock to synchronize access to the queue and running task
            async with self._lock:
                if not self.queue:
                    self.running_task = None
                    self._current_execution = None
                    self.is_queue_processing = False
                    break

                # Get the next task from the queue
                task = self.queue.popleft()
                self.running_task = task
                self._current_execution = asyncio.create_task(task.coroutine)

            try:
                await asyncio.wait_for(
                    asyncio.shield(self._current_execution), timeout=task.timeout
                )
                LOGGER.info(
                    "Finished: '%s' (Level %s)",
                    self.running_task.name,
                    self.running_task.level.name,
                )
            except asyncio.TimeoutError:
                LOGGER.warning(
                    "Timeout: '%s' (Level %s)",
                    self.running_task.name,
                    self.running_task.level.name,
                )
                self._current_execution.cancel()
            except asyncio.CancelledError:
                LOGGER.info(
                    "Aborted: '%s' (Level %s)",
                    self.running_task.name,
                    self.running_task.level.name,
                )
            except Exception as e:
                LOGGER.warning(
                    "Failed with Error: '%s' (Level %s): %s",
                    self.running_task.name,
                    self.running_task.level.name,
                    e,
                )
        # Invoke the callback if the queue is idle
        if callable(idle_callback):
            async with self._lock:
                still_idle = not self.queue and not self.is_queue_processing
            if still_idle:
                idle_callback()