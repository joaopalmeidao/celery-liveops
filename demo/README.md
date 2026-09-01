# The demo

A four-container stack — Redis, RabbitMQ, a Celery worker, a FastAPI panel —
built so you can break it on purpose and watch what the library shows you.

```bash
docker compose up --build
open http://localhost:8000
```

## Four things to try, in order

**1. Press "Healthy long task".**
Its terminal streams into the panel while it runs. Those log lines are written
by the worker container and read by the API container; nothing is polling a
file, and no `celery inspect` is involved.

**2. Press "Task that hangs".**
It never returns. Twenty seconds later the watchdog kills the process that is
running it — that deadline belongs to the *task*, not the container, so the
healthy long task next to it keeps its own five-minute budget. The row flips to
**no signal**, and the terminal of the dead attempt is *still readable*: it was
archived on the retry, not deleted.

**3. Press "Dies holding a lock".**
It takes `lock:import:acme` and exits without releasing it. In most systems this
is invisible — the next import "succeeds" instantly and nothing runs. Here the
lock shows up by name, with its remaining TTL and a Release button. Press the
button, then press the task again: it runs.

**4. `docker compose stop worker`.**
Every running row loses its signal at once. That is the difference between "this
task is taking a while" and "nobody is executing this and never will".

## Also worth noticing

- A queue named **`forgotten`** is declared and consumed by nobody. It shows up
  under Queues in red. That is the quietest failure a Celery deployment has:
  publish succeeds, the screen says "queued", nothing ever runs.
- **Stop Redis** (`docker compose stop redis`) while a task runs. The panel goes
  blank; the task keeps working and finishes. Observability degrades, the job
  does not.
- The **Last frame** thumbnail comes from `snapshot()`. This demo draws a PNG
  instead of driving a browser — the call is the same one you would make with
  `snapshot(driver)` after a click.

## What is not here

No database and no runs table. Every number on that page comes from Redis and
the broker. A real system has its own task table; the point is that the *live*
half does not need one.
