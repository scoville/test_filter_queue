# Test Task Queue

A serialized async priority task queue backed by `deque[QueuedTask]`. Producers submit coroutines via `ConditionalPreemptiveScheduler`; exactly one task runs at a time, with optional preemption, eviction, and per-task timeouts.

## Required Behaviors

| No. | Required Behavior |
| --- | --- |
| 1 | Only one task runs at a time. |
| 2 | Tasks from the queue do not interrupt a running task by default. If `can_interrupt_running=True`, a higher-level queued task can preempt the running task. |
| 3 | Higher-level incoming tasks evict all lower-level tasks from the queue. |
| 4 | Do not enqueue an incoming task if any **queued** or **running** task has a strictly higher level. |
| 5 | Stop a running task when its predefined `timeout` is exceeded, then continue processing the queue. |

Run the test suite:

```bash
uv run pytest -v
```
