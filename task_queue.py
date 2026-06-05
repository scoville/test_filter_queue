import asyncio
import logging
from collections import deque
from collections.abc import Coroutine
from dataclasses import dataclass
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

    level: TaskLevel
    name: str
    coroutine: Coroutine[Any, Any, Any]
    can_interrupt_running: bool = False
    timeout: float = 120

class FilterQueue:
    def __init__(self) -> None:
        self._queue: deque[QueuedTask] = deque()


    def submit_task(
        self,
        task: QueuedTask
    ) -> bool:
        """Enqueue a coroutine subject to priority rules.

        :param coroutine: Coroutine to run when scheduled by a consumer.
        :param level: Task level. Higher values dominate lower values.
        :param name: Task name for logging and debugging.
        :param can_interrupt_running: If True and level is strictly greater than the
            running level when, this task in the preempt slot instead of the deques.
        :return: True if accepted; False if rejected by rule 6 or stopped.
        """
        # Reject if a strictly higher rank is already at the front
        if self._queue and self._queue[0].level > task.level:
            LOGGER.warning(
                "Rejecting incoming lower-level task %s (level=%s).",
                task.name,
                task.level.name,
            )
            return False


        #  Evict all tasks from the back that have a strictly lower level
        while self._queue and self._queue[-1].level < task.level:
            evicted = self._queue.pop()
            LOGGER.debug(
                    "Evicted queued task %s (level=%s) from queue.",
                    evicted.name,
                    evicted.level.name,
                    extra={
                        "class_name": self.__class__.__name__,
                        "task_name": evicted.name,
                    },
                )
        
        self._queue.append(task)
        return True

    def peek_highest(self) -> QueuedTask | None:
        return self._queue[0] if self._queue else None

    def pop_highest(self) -> QueuedTask | None:
        return self._queue.popleft() if self._queue else None


class Worker:
    def __init__(self, queue: FilterQueue) -> None:
        self.queue = queue
        self.running_task: QueuedTask | None = None
        self._current_execution: asyncio.Task | None = None

    async def start_loop(self) -> None:
        LOGGER.info("Started monitoring loop...")
        try:
            while True:
                highest_queued = self.queue.peek_highest()

                # Case 1: Idle worker + task available -> Run it
                if self.running_task is None and highest_queued is not None:
                    self._execute_next()

                # Case 2: Active worker + higher rank available -> Preempt
                elif (
                    self.running_task is not None 
                    and highest_queued is not None 
                    and self.running_task.level < highest_queued.level
                    and highest_queued.can_interrupt_running
                ):
                    LOGGER.warning(
                        "PREEMPTION TRIPPED! Running: '%s' (Level %s) < Queued: '%s' (Level %s)",
                        self.running_task.name,
                        self.running_task.level.name,
                        highest_queued.name,
                        highest_queued.level.name,
                    )
                    
                    if self._current_execution:
                        self._current_execution.cancel()
                    
                    # Core swap happens immediately
                    self._execute_next()

                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            LOGGER.info("Monitoring loop stopped.")
    

    def _execute_next(self) -> None:
        task = self.queue.pop_highest()
        if task:
            LOGGER.info("Starting execution: '%s' (Level %s)", task.name, task.level.name)
            self.running_task = task
            self._current_execution = asyncio.create_task(self._run_with_timeout(task.coroutine, task.timeout))
            self._current_execution.add_done_callback(self._handle_execution_completion)
    
    async def _run_with_timeout(self, coroutine: Coroutine, timeout: float) -> Any:
        """Helper coroutine to enforce an execution time limit."""
        async with asyncio.timeout(timeout):
            return await coroutine

    def _handle_execution_completion(self, fut: asyncio.Task) -> None:
        try:
            fut.result()
            LOGGER.info("Finished: '%s' (Level %s)", self.running_task.name, self.running_task.level.name)
        except asyncio.TimeoutError:
            LOGGER.warning("Timeout: '%s' (Level %s)", self.running_task.name, self.running_task.level.name)
        except asyncio.CancelledError:
            LOGGER.info("Aborted: '%s' (Level %s)", self.running_task.name, self.running_task.level.name)
        except Exception as e:
            LOGGER.warning("Failed with Error: %s", self.running_task.name, self.running_task.level.name, e)
        finally:
            self.running_task = None
            self._current_execution = None


class FilterQueueConsumer:
    def __init__(self, queue: FilterQueue) -> None:
        self.worker = Worker(queue)
        self.worker_loop_task: asyncio.Task | None = None

    def start_worker(self) -> None:
        self.worker_loop_task = asyncio.create_task(self.worker.start_loop())

    def stop_worker(self) -> None:
        if self.worker_loop_task:
            self.worker_loop_task.cancel()
            self.worker_loop_task = None