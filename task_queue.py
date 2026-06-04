import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Coroutine
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Literal

from check_for_exceptions import check_for_exceptions

LOGGER = logging.getLogger(__name__)


class TaskLevel(IntEnum):
    """Level for queued async work. Higher values dominate lower values."""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


_LEVELS_DESC = tuple(sorted(TaskLevel, key=lambda p: p.value, reverse=True))


@dataclass
class QueuedTask:
    """A coroutine waiting to be executed by AsyncTaskQueue."""

    level: TaskLevel
    name: str
    coroutine: Coroutine[Any, Any, Any]
    can_interrupt_running: bool = False


class AsyncTaskFilterQueue:
    """Run async coroutines strictly one at a time with level ordering.

    Waiting work is stored in ``dict[TaskLevel, deque[QueuedTask]]``. The worker
    uses ``popleft`` from the highest non-empty level deque plus an
    :class:`asyncio.Event` so preempted work does not block behind waiting tasks.
    """

    def __init__(
        self,
        task_done_log_exception_lvl: Literal["EXCEPTION", "ERROR", "INFO", "DEBUG"] = "EXCEPTION",
    ) -> None:
        """Constructor.

        :param task_done_log_exception_lvl: Log level for exceptions when a task completes.
            Defaults to "EXCEPTION".
        """
        self._queues: dict[TaskLevel, deque[QueuedTask]] = {
            level: deque() for level in TaskLevel
        }
        self._lock = asyncio.Lock()
        self._work_available = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._stopped = False

        self._running_task: asyncio.Task[None] | None = None
        self._running_level: TaskLevel | None = None
        self._preempt: QueuedTask | None = None

        self.task_done_log_exception_lvl = task_done_log_exception_lvl

    @property
    def is_running(self) -> bool:
        """Check whether a task is currently executing."""
        return self._running_task is not None and not self._running_task.done()

    @property
    def queue_size(self) -> int:
        """Return the number of tasks waiting in the level deques."""
        return self._waiting_count()

    @staticmethod
    def _close_coroutine(coroutine: Coroutine[Any, Any, Any]) -> None:
        coroutine.close()

    def _close_task(self, task: QueuedTask) -> None:
        self._close_coroutine(task.coroutine)

    def _has_waiting(self) -> bool:
        return any(self._queues[level] for level in _LEVELS_DESC)

    def _waiting_count(self) -> int:
        return sum(len(queue) for queue in self._queues.values())

    def _highest_queued_level(self) -> TaskLevel | None:
        for level in _LEVELS_DESC:
            if self._queues[level]:
                return level
        return None

    def _dominant_level(self) -> TaskLevel | None:
        queued = self._highest_queued_level()
        if self._running_level is not None:
            if queued is None or self._running_level > queued:
                return self._running_level
            return queued
        return queued

    def _evict_queued_below(self, incoming_level: TaskLevel) -> None:
        for level in TaskLevel:
            if level >= incoming_level:
                continue
            while self._queues[level]:
                discarded = self._queues[level].popleft()
                LOGGER.debug(
                    "Evicting queued task %s (level=%s)",
                    discarded.name,
                    discarded.level.name,
                    extra={
                        "class_name": self.__class__.__name__,
                        "task_name": discarded.name,
                    },
                )
                self._close_task(discarded)

    def _pop_highest_waiting(self) -> QueuedTask | None:
        for level in _LEVELS_DESC:
            if self._queues[level]:
                return self._queues[level].popleft()
        return None

    def _drain_all_waiting(self) -> None:
        for level in TaskLevel:
            while self._queues[level]:
                self._close_task(self._queues[level].popleft())

    def _ensure_worker(self) -> None:
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker(), name="task_queue_worker")

    async def submit_async(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        level: TaskLevel = TaskLevel.NORMAL,
        name: str = "unknown",
        can_interrupt_running: bool = False,
    ) -> bool:
        """Enqueue a coroutine for serialized execution.

        :param coroutine: Coroutine to run when scheduled.
        :param level: Task level. Higher values dominate lower values.
        :param name: Task name for logging and debugging.
        :param can_interrupt_running: If True and level is strictly greater than the
            running task, cancel the running task and run this task next. Defaults to False.
        :return: True if accepted; False if rejected by rule 6.
        """
        incoming = QueuedTask(
            level=level,
            name=name,
            coroutine=coroutine,
            can_interrupt_running=can_interrupt_running,
        )

        async with self._lock:
            if self._stopped:
                self._close_task(incoming)
                return False

            dominant_level = self._dominant_level()

            if dominant_level is not None and incoming.level < dominant_level:
                LOGGER.warning(
                    "Rejecting lower-level task %s (level=%s); dominant level is %s",
                    name,
                    level.name,
                    dominant_level.name,
                    extra={
                        "class_name": self.__class__.__name__,
                        "task_name": name,
                        "task_level": level.name,
                        "dominant_level": dominant_level.name,
                    },
                )
                self._close_task(incoming)
                return False

            should_interrupt = (
                self.is_running
                and can_interrupt_running
                and self._running_level is not None
                and incoming.level > self._running_level
            )

            if should_interrupt and self._running_task is not None:
                LOGGER.debug(
                    "Interrupting running task for higher-level task %s",
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
                self._evict_queued_below(incoming.level)
                self._queues[incoming.level].append(incoming)

            self._ensure_worker()
            self._work_available.set()
            return True

    async def cancel(self) -> None:
        """Cancel the running task, drain the queue, and stop the worker."""
        async with self._lock:
            self._stopped = True
            if self._running_task is not None and not self._running_task.done():
                self._running_task.cancel()
            self._drain_all_waiting()
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

    async def _worker(self) -> None:
        while True:
            entry: QueuedTask | None = None
            async with self._lock:
                if self._stopped and self._preempt is None and not self._has_waiting():
                    return
                if self._preempt is not None:
                    entry = self._preempt
                    self._preempt = None
                else:
                    entry = self._pop_highest_waiting()

            if entry is None:
                self._work_available.clear()
                await self._work_available.wait()
                continue

            await self._execute(entry)

    async def _execute(self, entry: QueuedTask) -> None:
        self._running_level = entry.level
        self._running_task = asyncio.create_task(entry.coroutine, name=entry.name)
        self._running_task.add_done_callback(self._done_callback)
        try:
            await self._running_task
        except asyncio.CancelledError:
            pass
        finally:
            if self._running_task is not None and self._running_task.done():
                self._running_task = None
            self._running_level = None

    def _done_callback(self, task: asyncio.Task[None]) -> None:
        check_for_exceptions(task, self.task_done_log_exception_lvl, self.__class__.__name__)
