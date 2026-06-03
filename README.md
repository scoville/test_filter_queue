# Test Priority Queue
This is repo to test the usage of priority queue through asyncio.PriorityQueue to see whether it fulfill our expected Rule:

Expected Rule

| Rule       | Behavior                                                                                                                                                             |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1          | At most one running task; no interrupt by default                                                                                                                    |
| 1 (opt-in) | `can_interrupt_running=True` **and** `new_priority > running_priority` → cancel running, run new task next                                                           |
| 2          | Higher priority alone never cancels a running task                                                                                                                   |
| 3          | On enqueue, remove all **queued** tasks with priority **strictly less** than the incoming task                                                                       |
| 4–5        | Waiting tasks: max priority first; ties broken by FIFO (`sequence` counter)                                                                                          |
| 6          | Do **not** enqueue an incoming task if **any** task currently **running** or **queued** has **strictly higher** priority; close the coroutine and drop (log WARNING) |


To test whether fulfill those rules, please run:
```bash
uv run pytest -v
```