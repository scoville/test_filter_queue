import asyncio
import logging
import threading
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

class FilterQueue:
    def __init__(self) -> None:
        self._queue: deque[QueuedTask] = deque()
        self._lock = threading.Lock()

    def submit_task(
        self,
        task: QueuedTask
    ) -> bool:
        """Enqueue a incomming task.

        :param task: incoming task to be enqueued.
        :return: True if accepted; False if rejected.
        """
        with self._lock:
            # Reject if a strictly higher rank is already at the front
            if self._queue and self._queue[0].level > task.level:
                LOGGER.warning(
                    "Rejecting incoming lower-level task %s (level=%s).",
                    task.name,
                    task.level.name,
                )
                return False

            # Evict all tasks from the back that have a strictly lower level
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
        with self._lock:
            return self._queue[0] if self._queue else None

    def pop_highest(self) -> QueuedTask | None:
        with self._lock:
            return self._queue.popleft() if self._queue else None

    def pop_highest_if(
        self, should_pop: Callable[[QueuedTask], bool]
    ) -> QueuedTask | None:
        """Atomically inspect the front task and dequeue it if `should_pop` is true."""
        with self._lock:
            if not self._queue:
                return None
            head = self._queue[0]
            if not should_pop(head):
                return None
            return self._queue.popleft()


class Worker:
    def __init__(self, queue: FilterQueue) -> None:
        self.queue = queue
        self.running_task: QueuedTask | None = None
        self._current_execution: asyncio.Task | None = None

    async def start_loop(self) -> None:
        LOGGER.info("Started monitoring loop...")
        try:
            while True:
                running = self.running_task

                # Case 1: Idle worker + task available -> Run it
                if running is None:
                    task = self.queue.pop_highest()
                    if task:
                        self._start_task(task)

                # Case 2: Active worker + higher rank available -> Preempt
                else:
                    task = self.queue.pop_highest_if(
                        lambda head: (
                            running.level < head.level and head.can_interrupt_running
                        )
                    )
                    if task:
                        LOGGER.warning(
                            "PREEMPTION TRIPPED! Running: '%s' (Level %s) < Queued: '%s' (Level %s)",
                            running.name,
                            running.level.name,
                            task.name,
                            task.level.name,
                        )

                        if self._current_execution:
                            self._current_execution.cancel()

                        self._start_task(task)

                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            LOGGER.info("Monitoring loop stopped.")
    

    def _start_task(self, task: QueuedTask) -> None:
        LOGGER.info("Starting execution: '%s' (Level %s)", task.name, task.level.name)
        self.running_task = task
        self._current_execution = asyncio.create_task(
            self._run_with_timeout(task.coroutine, task.timeout)
        )
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