import asyncio
import contextlib
import logging
from collections.abc import Coroutine
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Literal
from check_for_exceptions import check_for_exceptions

LOGGER = logging.getLogger(__name__)


class TaskPriority(IntEnum):
    """Priority for queued async work. Higher values run before lower values."""

    LOW = 0
    NORMAL = 10
    HIGH = 20
    CRITICAL = 30


@dataclass
class QueuedTask:
    """A coroutine waiting to be executed by AsyncPriorityTaskQueue."""

    priority: TaskPriority
    name: str
    coroutine: Coroutine[Any, Any, Any]
    can_interrupt_running: bool = False
    sequence: int = field(default=0, compare=False)


class AsyncPriorityTaskQueue:
    """Run async coroutines strictly one at a time with priority ordering.

    Waiting work is stored in :class:`asyncio.PriorityQueue`. Rules 3 and 6 require
    draining and filtering that queue on submit; the worker uses ``get_nowait`` plus
    an :class:`asyncio.Event` so preempted work does not block behind ``PriorityQueue.get``.
    """

    def __init__(
        self,
        task_done_log_exception_lvl: Literal["EXCEPTION", "ERROR", "INFO", "DEBUG"] = "EXCEPTION",
    ) -> None:
        """Constructor.

        :param task_done_log_exception_lvl: Log level for exceptions when a task completes.
            Defaults to "EXCEPTION".
        """
        self._queue: asyncio.PriorityQueue[tuple[int, int, QueuedTask]] = asyncio.PriorityQueue()
        self._lock = asyncio.Lock()
        self._work_available = asyncio.Event()
        self._sequence = 0
        self._worker_task: asyncio.Task[None] | None = None
        self._stopped = False

        self._running_task: asyncio.Task[None] | None = None
        self._running_priority: TaskPriority | None = None
        self._preempt: QueuedTask | None = None

        self._async_to_sync_tasks: set[asyncio.Task[Any]] = set()
        self.task_done_log_exception_lvl = task_done_log_exception_lvl

    @property
    def is_running(self) -> bool:
        """Check whether a task is currently executing."""
        return self._running_task is not None and not self._running_task.done()

    @property
    def queue_size(self) -> int:
        """Return the number of tasks waiting in the priority queue."""
        return self._queue.qsize()

    def _next_sequence(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence

    @staticmethod
    def _sort_key(priority: TaskPriority, sequence: int) -> tuple[int, int]:
        return (-priority.value, sequence)

    @staticmethod
    def _close_coroutine(coroutine: Coroutine[Any, Any, Any]) -> None:
        coroutine.close()

    def _close_task(self, task: QueuedTask) -> None:
        self._close_coroutine(task.coroutine)

    async def _drain_waiting(self) -> list[QueuedTask]:
        waiting: list[QueuedTask] = []
        while not self._queue.empty():
            _, _, queued = self._queue.get_nowait()
            waiting.append(queued)
        return waiting

    async def _refill_waiting(self, tasks: list[QueuedTask]) -> None:
        for task in tasks:
            sort_key = self._sort_key(task.priority, task.sequence)
            await self._queue.put((*sort_key, task))

    @staticmethod
    def _dominant_priority(
        waiting: list[QueuedTask], running_priority: TaskPriority | None
    ) -> TaskPriority | None:
        priorities: list[TaskPriority] = [task.priority for task in waiting]
        if running_priority is not None:
            priorities.append(running_priority)
        if not priorities:
            return None
        return max(priorities, key=lambda value: value.value)

    def _ensure_worker(self) -> None:
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(
                self._worker(), name="priority_task_queue_worker"
            )

    def _async_to_sync_done_callback(self, task: asyncio.Task[Any]) -> None:
        self._async_to_sync_tasks.discard(task)
        check_for_exceptions(task, self.task_done_log_exception_lvl, self.__class__.__name__)

    async def submit_async(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        priority: TaskPriority = TaskPriority.NORMAL,
        name: str = "unknown",
        can_interrupt_running: bool = False,
    ) -> bool:
        """Enqueue a coroutine for serialized execution.

        :param coroutine: Coroutine to run when scheduled.
        :param priority: Task priority. Higher values run before lower values.
        :param name: Task name for logging and debugging.
        :param can_interrupt_running: If True and priority is strictly greater than the
            running task, cancel the running task and run this task next. Defaults to False.
        :return: True if accepted; False if rejected by rule 6.
        """
        incoming = QueuedTask(
            priority=priority,
            name=name,
            coroutine=coroutine,
            can_interrupt_running=can_interrupt_running,
            sequence=self._next_sequence(),
        )

        async with self._lock:
            if self._stopped:
                self._close_task(incoming)
                return False

            waiting = await self._drain_waiting()
            dominant = self._dominant_priority(waiting, self._running_priority)

            if dominant is not None and incoming.priority < dominant:
                LOGGER.warning(
                    "Rejecting lower-priority task %s (priority=%s); dominant priority is %s",
                    name,
                    priority.name,
                    dominant.name,
                    extra={
                        "class_name": self.__class__.__name__,
                        "task_name": name,
                        "task_priority": priority.name,
                        "dominant_priority": dominant.name,
                    },
                )
                self._close_task(incoming)
                await self._refill_waiting(waiting)
                return False

            should_interrupt = (
                self.is_running
                and can_interrupt_running
                and self._running_priority is not None
                and incoming.priority > self._running_priority
            )

            if should_interrupt and self._running_task is not None:
                LOGGER.debug(
                    "Interrupting running task for higher-priority task %s",
                    name,
                    extra={"class_name": self.__class__.__name__, "task_name": name},
                )
                self._running_task.cancel()
                try:
                    await self._running_task
                except asyncio.CancelledError:
                    pass
                except Exception:  # noqa: BLE001
                    pass
                self._preempt = incoming
            else:
                kept = [task for task in waiting if task.priority >= incoming.priority]
                for discarded in waiting:
                    if discarded not in kept:
                        LOGGER.debug(
                            "Evicting queued task %s (priority=%s)",
                            discarded.name,
                            discarded.priority.name,
                            extra={
                                "class_name": self.__class__.__name__,
                                "task_name": discarded.name,
                            },
                        )
                        self._close_task(discarded)
                kept.append(incoming)
                await self._refill_waiting(kept)

            self._ensure_worker()
            self._work_available.set()
            return True

    def submit(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        priority: TaskPriority = TaskPriority.NORMAL,
        name: str = "unknown",
        can_interrupt_running: bool = False,
    ) -> None:
        """Submit a coroutine from a synchronous context (fire-and-forget).

        :param coroutine: Coroutine to run when scheduled.
        :param priority: Task priority. Higher values run before lower values.
        :param name: Task name for logging and debugging.
        :param can_interrupt_running: If True and priority is strictly greater than the
            running task, cancel the running task and run this task next. Defaults to False.
        """
        creation_task = asyncio.create_task(
            self.submit_async(
                coroutine, priority=priority, name=name, can_interrupt_running=can_interrupt_running
            )
        )
        self._async_to_sync_tasks.add(creation_task)
        creation_task.add_done_callback(self._async_to_sync_done_callback)

    async def cancel(self) -> None:
        """Cancel the running task, drain the queue, and stop the worker."""
        async with self._lock:
            self._stopped = True
            if self._running_task is not None and not self._running_task.done():
                self._running_task.cancel()
            waiting = await self._drain_waiting()
            for task in waiting:
                self._close_task(task)
            if self._preempt is not None:
                self._close_task(self._preempt)
                self._preempt = None
            self._work_available.set()

        if self._running_task is not None:
            try:
                await self._running_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                pass

        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task

        for submit_task in list(self._async_to_sync_tasks):
            submit_task.cancel()

    async def _worker(self) -> None:
        while True:
            entry: QueuedTask | None = None
            async with self._lock:
                if self._stopped and self._preempt is None and self._queue.empty():
                    return
                if self._preempt is not None:
                    entry = self._preempt
                    self._preempt = None
                else:
                    try:
                        _, _, entry = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        entry = None

            if entry is None:
                self._work_available.clear()
                await self._work_available.wait()
                continue

            await self._execute(entry)

    async def _execute(self, entry: QueuedTask) -> None:
        self._running_priority = entry.priority
        self._running_task = asyncio.create_task(entry.coroutine, name=entry.name)
        self._running_task.add_done_callback(self._done_callback)
        try:
            await self._running_task
        except asyncio.CancelledError:
            pass
        finally:
            if self._running_task is not None and self._running_task.done():
                self._running_task = None
            self._running_priority = None

    def _done_callback(self, task: asyncio.Task[None]) -> None:
        check_for_exceptions(task, self.task_done_log_exception_lvl, self.__class__.__name__)
