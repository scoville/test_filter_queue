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



    def _ensure_worker(self) -> None:
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker(), name="task_queue_worker")



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