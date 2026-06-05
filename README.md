# Test Task Queue

This repo tests a serialized async task queue backed by `deque[QueuedTask]` to see whether it fulfills our required behavior:

## Required Behaviors

| No.   | Required Behavior                                                                                                                                                             |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1          | Only one task runs at a time.                                                                                                                |
| 2          | Task from queue do not interrupt running task by default. However, if `can_interrupt_running=True`, higher level task from queue can interrupt running task                                                          |
| 3          | Higher-level incoming task evict all lower-level tasks from queue                                                                                    |
| 4          | Do not enqueue an incoming task if any task queued has strictly higher level |
| 5          | Interrupt running task if its predefined task timeout happens |

To test whether those required behaviors are fulfilled, run:

```bash
uv run pytest -v
```
