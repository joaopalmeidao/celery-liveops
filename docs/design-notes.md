# Design notes

Why the less obvious choices are what they are. Most of them were paid for by an
incident.

---

## The one rule

**Observability must never break the job it is observing.**

Every Redis call in this library is wrapped. Every failure degrades the panel and
never the work. That sounds like ordinary defensive programming until you notice
what it forbids: no read may raise into your endpoint, no write may raise into
your task, and no failure may be so loud that it floods the very log it is
trying to capture (which is why `store.guard` logs at DEBUG, not WARNING).

It also decides how each function fails, and the answers are not all the same —
see *Fail open, fail closed* below.

---

## Logs

### The previous attempt's buffer is archived, not deleted

Celery keeps the task id across retries. That single fact creates a trap.

A task dies without running its `finally` — OOM, a watchdog, a deploy mid-run.
Its log lines exist nowhere but Redis. The retry starts, begins capturing under
the *same key*, and the first thing the naive implementation does is clear the
buffer — destroying the only evidence of why the first attempt died.

So `capture_logs` renames the buffer instead:

```
liveops:log:{id}  ->  liveops:log:{id}:orphan   (TTL 15 min)
```

Your reaper reads it with `read_orphan_logs(id)`, archives it against the failed
row, and closes it. The retry still starts with a clean terminal — that part of
the contract is unchanged.

Three details in the fallback path are deliberate:

- If `RENAME` fails, we fall back to `DELETE`. Interleaving two attempts' logs
  is worse than losing one.
- The common case (nothing to preserve) costs **one** call — an `EXISTS` — the
  same as the `DELETE` it replaced. With Redis unreachable, every call costs the
  client's full timeout; a naive "try rename, else delete" would double that
  wait on every task in the system.
- `read_any_logs` reads both keys in one pipeline, because its caller is usually
  serving a *finished* run, which is exactly when Redis is least likely to be
  reachable.

### Extra keys stack; the root does not move

A task that processes a batch wants one terminal for the whole batch *and* a
per-item slice to archive. `capture_logs(key, extra=True)` writes the same lines
to both.

The root key is deliberately left alone, because `current_key()` is what
cooperative cancellation and live snapshots key on, and those need the real task
id — not the id of the item being processed.

### The handler is a no-op outside a run

`LiveLogHandler.emit` returns immediately when the ContextVar is empty. That is
what lets you attach it once, in your shared logging setup, and use the same
configuration in your API, your beat process and your workers.

---

## Presence

### Signals and a refresh thread, never `celery inspect`

`inspect` broadcasts on the broker and waits for replies. It is slow, it times
out under load, and the caller here is an endpoint being polled every few
seconds by every open browser tab. So the worker *writes* presence and readers
only ever do `EXISTS`.

The refresh thread belongs to the **process**, not to a task. That distinction
cost a broken CI run: with the thread scoped to a task, a worker only had a
heartbeat while it happened to be busy, so the panel reported zero workers the
moment the queue drained — and anything waiting for a worker to appear before
queuing work waited forever. `worker_ready` announces the process, the thread
keeps it warm, `worker_shutdown` withdraws it.

### Everything has a TTL

A process that dies leaves nothing behind claiming to be alive. Presence that
needed cleanup would be a second thing to reap, and the whole point is to detect
the case where cleanup did not run.

The TTL is generous relative to the refresh interval (90s vs 20s): one network
hiccup must not mark a healthy run as dead.

### `worker_id()` is computed per call

In a prefork worker, this module is imported in the parent, *before* the fork. A
value cached at import is inherited by every child, so all of them write to the
same key and overwrite each other — and your panel shows one worker instead of
eight. The same reasoning applies to the Redis client, which is why `store.py`
builds it lazily.

---

## The watchdog

### The deadline belongs to the task, not the container

Celery enforces `task_time_limit` by killing the child process of a prefork
pool. With `--pool=solo` there is no child; the limit silently never fires.

The obvious fix is an environment variable per service. That works, and it costs
you your topology: a 15-minute timeout and an 8-hour timeout cannot live in the
same container, so every duration profile needs its own service, and a pool of
identical replicas can never serve both — the overnight run dies halfway through
inside the short-timeout container.

Registering the deadline per **task name** collapses that. One fungible pool
serves the quick job and the long one, and scaling becomes a single number.

### It is off by default

A library that calls `os._exit` has to be opted into. Installing it must never be
enough to make it fire.

### `os._exit`, not a signal

It runs from the timer thread, needs no signal delivery (`SIGKILL` does not exist
on Windows), and does not wait for a process that is, by definition, not
answering. The cost is real and should be stated plainly: **the task's `finally`
does not run.** Whatever row you opened stays open, and a reaper has to close it.

Which is why cooperative stopping is better wherever the task can manage it.
`safe_stop_at()` returns 90% of the budget; the remaining 10% pays for closing
the browser, writing a checkpoint and marking the run finished. The watchdog is
then only the backstop for code that cannot stop itself.

---

## Locks

### An allowlist, because the key comes from a browser

The release endpoint receives a raw Redis key from the client. Without
`register_lock`, an arbitrary `DEL` against production Redis is one POST away.
A key that is not in the catalogue is refused — and refusing is not an error:
in a bulk release, one unknown key must not take the others down with it.

### Automatic release only for singleton locks

Killing a run does not prove which lock was its own. `lock:login:acct-42` might
belong to a *live* run on the same account, and freeing that one is worse than
letting an orphan expire on its own. So only wildcard-free patterns — one global
key, one unambiguous owner — are released automatically.

### `blocks` is a sentence, not a label

The field exists because the failure it describes is invisible. Nothing errors:
the trigger "works", the task exits with "already running", the screen shows
nothing. The operator reading that row at 2am needs to know what *stopped
happening*, not what the key is called.

---

## Queues, and failing in the right direction

Two measurements, two opposite failure modes, both on purpose:

| Call | On failure | Why |
|---|---|---|
| `queue_depth` | `None` | A reaper deciding whether a pending item was abandoned must distinguish "the queue is empty, so nobody is coming" from "I could not ask". `None` is not zero; conflating them invents failures that never happened. |
| `has_consumer` | `True` | Callers use it to fail fast *before* enqueueing. Refusing legitimate work because the broker was briefly slow is worse than the wait. |
| `orphan_queues` | `[]` | With no measurement you accuse nothing, or the alert fires on every broker blip. |

`orphan_queues` deserves its own note. A declared queue with no consumer is the
quietest failure a Celery deployment has: publish succeeds, your screen says
"queued", and nothing ever runs it. It happens the day a service is commented out
of the compose file to free memory, and it stays invisible until somebody asks
where last Tuesday's report went.

---

## Scale

### Refusing beats lying

`pool_grow` on a solo pool is *accepted* and does nothing. A UI built on that
shows a new number that is not real. `apply_target` returns `False` instead.

### Under `--autoscale`, the number means the ceiling

Demand sizes the pool; growing it directly would be undone at the first idle
moment. So the operator's number is applied through `Autoscaler.update(max=...)`,
which also shrinks immediately when the live pool is above it.

### There is an absolute ceiling

Each executor of a browser worker is a Chrome process at roughly a gigabyte. A
mistyped number is an out-of-memory kill requested in advance.

---

## Snapshots

Capture is synchronous because a WebDriver is not thread-safe. Everything after
that — base64, the Redis write — happens on a background thread and is coalesced:
if a store is still in flight, the new frame is dropped, because the next one
covers it anyway. Throttling (about one per 1.2s) keeps a call inside a hot loop
from competing with the work it is watching.

Resolving the source object checks capability first — "does it know how to take a
screenshot?" — never attribute names. Since Selenium 4.30 a WebDriver exposes a
`browser` property (the BiDi module) that *raises* when BiDi is unavailable, and
`getattr(x, "browser", None)` only swallows `AttributeError`.

Failures are counted and the last error is kept. A bare `except: return` makes a
broken feature indistinguishable from an idle one.

---

## What this library deliberately does not do

- **No database.** Your runs table is yours. This handles the live half; `cap_log`
  exists so you can archive a terminal into a column you own.
- **No UI.** `contrib/fastapi` is a router, not a dashboard. The demo's page is a
  demo.
- **No opinion about your task ids.** Every function takes a key. It is usually
  `self.request.id`, but a batch item id works exactly as well.
