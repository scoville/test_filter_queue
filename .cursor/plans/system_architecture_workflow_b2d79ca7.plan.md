---
name: System Architecture Workflow
overview: A read-only System Architecture Workflow for the serialized async priority task queue in [task_queue.py](task_queue.py), covering components, data flow, concurrency guarantees, and all five required behaviors from [README.md](README.md).
todos: []
isProject: false
---

# System Architecture Workflow

## High-Level Overview

This system is a **single-worker, serialized async task queue** with priority filtering. Producers submit `QueuedTask` coroutines; one `Worker` dequeues and runs exactly one task at a time, with optional preemption and per-task timeouts.

```mermaid
flowchart TB
    subgraph producers [Producers]
        App[Application / Tests]
    end

    subgraph queueLayer [FilterQueue]
        Lock[threading.Lock]
        Deque["deque of QueuedTask"]
        Submit[submit_task]
        Pop[pop_highest / pop_highest_if]
    end

    subgraph consumerLayer [FilterQueueConsumer]
        Start[start_worker]
        Stop[stop_worker]
        Worker[Worker]
    end

    subgraph execution [Execution]
        MonitorLoop[start_loop polling]
        StartTask[_start_task]
        RunTimeout[_run_with_timeout]
        Callback[_handle_execution_completion]
    end

    App -->|QueuedTask| Submit
    Submit --> Deque
    Deque --> Lock
    Start --> MonitorLoop
    MonitorLoop --> Pop
    Pop --> StartTask
    StartTask --> RunTimeout
    RunTimeout --> Callback
    Callback -->|running_task = None| MonitorLoop
    Stop -->|cancel| MonitorLoop
```

## Core Components

| Component | Responsibility |
|-----------|----------------|
| [`TaskLevel`](task_queue.py) | Priority enum: `LOW(0) < NORMAL(1) < HIGH(2) < CRITICAL(3)` |
| [`QueuedTask`](task_queue.py) | Task payload: `level`, `name`, `coroutine`, `can_interrupt_running`, `timeout` |
| [`FilterQueue`](task_queue.py) | Thread-safe priority-filtered `deque`; accepts/rejects/evicts on submit |
| [`Worker`](task_queue.py) | Polls queue, runs one task, handles preemption and completion |
| [`FilterQueueConsumer`](task_queue.py) | Lifecycle wrapper: starts/stops the worker asyncio task |

## Lifecycle: Startup to Shutdown

```mermaid
sequenceDiagram
    participant App
    participant Consumer as FilterQueueConsumer
    participant Worker
    participant Queue as FilterQueue
    participant Exec as asyncio.Task

    App->>Consumer: FilterQueueConsumer(queue)
    App->>Consumer: start_worker()
    Consumer->>Worker: create_task(start_loop)

    loop Every 100ms
        Worker->>Queue: pop_highest or pop_highest_if
        alt task available
            Worker->>Worker: _start_task(task)
            Worker->>Exec: create_task(_run_with_timeout)
            Exec-->>Worker: done_callback
            Worker->>Worker: running_task = None
        end
        Worker->>Worker: await sleep(0.1)
    end

    App->>Consumer: stop_worker()
    Consumer->>Worker: cancel start_loop
```

## Workflow 1: Task Submission (`submit_task`)

All submission logic runs under `threading.Lock` in [`FilterQueue.submit_task`](task_queue.py).

```mermaid
flowchart TD
    Start[submit_task called] --> Lock[Acquire _lock]
    Lock --> CheckFront{Front task level > incoming level?}
    CheckFront -->|Yes| Reject[Return False - Behavior 4]
    CheckFront -->|No| EvictLoop{Back task level < incoming level?}
    EvictLoop -->|Yes| PopBack[pop from back - Behavior 3]
    PopBack --> EvictLoop
    EvictLoop -->|No| Append[append to back]
    Append --> Accept[Return True]
    Reject --> Unlock[Release _lock]
    Accept --> Unlock
```

**Priority rules on enqueue:**
- **Reject (Behavior 4):** If queue front has strictly higher level than incoming task, reject.
- **Evict (Behavior 3):** Remove all strictly lower-level tasks from the back before appending.
- **Append:** New task always goes to the back; front remains highest-priority waiting task.

**Note:** Queue ordering assumes front = next to run. Higher-priority tasks submitted later evict lower ones from the back but do not automatically jump ahead of equal/higher front tasks.

## Workflow 2: Worker Monitor Loop (`start_loop`)

The worker polls every **100ms** and branches on whether a task is currently running.

```mermaid
flowchart TD
    LoopStart[Loop iteration] --> ReadRunning[running = self.running_task]

    ReadRunning --> IdleCheck{running is None?}
    IdleCheck -->|Yes| PopIdle["pop_highest() - atomic dequeue"]
    PopIdle --> HasTask1{task returned?}
    HasTask1 -->|Yes| Start1[_start_task]
    HasTask1 -->|No| Sleep

    IdleCheck -->|No| PopPreempt["pop_highest_if(predicate) - atomic inspect+dequeue"]
    PopPreempt --> PreemptCheck{head.level > running.level AND can_interrupt_running?}
    PreemptCheck -->|Yes| Cancel[cancel _current_execution]
    Cancel --> Start2[_start_task - Behavior 2]
    PreemptCheck -->|No| Sleep[await sleep 0.1]
    Start1 --> Sleep
    Start2 --> Sleep
    Sleep --> LoopStart
```

**Behavior mapping:**
- **Behavior 1 (one at a time):** Only one `_current_execution` asyncio task active; new work waits until `running_task` is cleared.
- **Behavior 2 (preemption):** Higher-level queued task with `can_interrupt_running=True` atomically dequeued and running task cancelled.
- **Behavior 2 inverse:** Lower-level tasks never preempt, even with `can_interrupt_running=True`.

## Workflow 3: Task Execution (`_start_task` → `_run_with_timeout`)

```mermaid
sequenceDiagram
    participant Worker
    participant AsyncTask as asyncio.Task
    participant Coroutine

    Worker->>Worker: running_task = task
    Worker->>AsyncTask: create_task(_run_with_timeout)
    Worker->>AsyncTask: add_done_callback

    AsyncTask->>AsyncTask: async with asyncio.timeout(timeout)
    AsyncTask->>Coroutine: await coroutine

    alt completes in time
        Coroutine-->>AsyncTask: return
        AsyncTask-->>Worker: Finished log
    else exceeds timeout
        AsyncTask-->>Worker: TimeoutError - Behavior 5
    else preempted
        AsyncTask-->>Worker: CancelledError
    else coroutine error
        AsyncTask-->>Worker: Exception log
    end

    Worker->>Worker: finally: running_task = None
```

**Behavior 5 (timeout):** `asyncio.timeout(timeout)` wraps the coroutine. On timeout, `TimeoutError` is caught, worker is freed in `finally`, and the monitor loop can dequeue the next task.

## Workflow 4: Concurrency Model

```mermaid
flowchart LR
    subgraph threadSafe [Thread-safe queue ops]
        T1[Any OS thread]
        T2[Event loop thread]
        T1 -->|submit_task| Lock
        T2 -->|pop_highest / pop_highest_if| Lock
        Lock --> Deque
    end

    subgraph atomic [Peek-pop race fix]
        OldRisk["OLD: peek_highest then pop_highest - gap between lock releases"]
        NewFix["NEW: pop_highest_if - single lock scope for inspect+dequeue"]
    end
```

| Concern | Mechanism |
|---------|-----------|
| Cross-thread `submit_task` | `threading.Lock` on all deque mutations |
| Peek-then-pop mismatch | `pop_highest_if()` — inspect and dequeue atomically |
| Single-threaded asyncio | No `await` between dequeue decision and `_start_task` in one loop iteration |
| Worker responsiveness | 100ms polling via `asyncio.sleep(0.1)` (not event-driven wakeup yet) |

## End-to-End Example (Preemption + Eviction)

```mermaid
sequenceDiagram
    participant App
    participant Queue
    participant Worker

    App->>Queue: submit LOW running task
    Worker->>Worker: starts LOW
    App->>Queue: submit HIGH can_interrupt=True
    Note over Queue: HIGH appended to queue
    Worker->>Queue: pop_highest_if preempt predicate
    Queue-->>Worker: HIGH atomically dequeued
    Worker->>Worker: cancel LOW, start HIGH
    App->>Queue: submit CRITICAL
    Note over Queue: evicts HIGH from back, queue = CRITICAL
    Worker->>Worker: HIGH completes
    Worker->>Queue: pop_highest
    Queue-->>Worker: CRITICAL
```

## State Machine (Worker)

```mermaid
stateDiagram-v2
    [*] --> Idle: start_loop
    Idle --> Running: pop_highest returns task
    Running --> Idle: task completes / timeout / error
    Running --> Running: preempt higher task
    Idle --> Idle: queue empty
    Running --> Idle: cancelled by preempt
    Idle --> [*]: stop_worker cancel
    Running --> [*]: stop_worker cancel
```

## Key Files

- Implementation: [task_queue.py](task_queue.py)
- Behavior specs + test command: [README.md](README.md)
- Verification: [test_task_queue.py](test_task_queue.py) (7 tests covering behaviors 1–5)

## Known Design Notes

- `peek_highest()` remains for read-only inspection but is **not** used by the worker loop (avoids TOCTOU race).
- `submit_task` is **sync** by design — fast in-memory enqueue, safe with `threading.Lock`.
- Worker polls every 100ms rather than using an `asyncio.Event` wakeup (acceptable latency tradeoff).
- Placeholder `pass` in `_handle_execution_completion` (lines 175–178) suggests future integration hooks (e.g. turn-taking / mic control) — not part of current queue behavior.
