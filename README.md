# Test Task Queue

This repo tests a serialized async task queue backed by `dict[TaskPriority, deque[QueuedTask]]` to see whether it fulfills our expected rules:

## Expected Rules

| Rule       | Behavior                                                                                                                                                             |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1          | At most one running task; no interrupt by default                                                                                                                    |
| 1 (opt-in) | `can_interrupt_running=True` **and** `new_priority > running_priority` → cancel running, run new task next                                                           |
| 2          | Higher priority alone never cancels a running task                                                                                                                   |
| 3          | On enqueue, remove all **queued** tasks with priority **strictly less** than the incoming task                                                                       |
| 4–5        | Waiting tasks: max priority first; ties broken by FIFO (per-priority `deque`)                                                                                        |
| 6          | Do **not** enqueue an incoming task if **any** task currently **running** or **queued** has **strictly higher** priority; close the coroutine and drop (log WARNING) |

To test whether those rules are fulfilled, run:

```bash
uv run pytest -v
```
