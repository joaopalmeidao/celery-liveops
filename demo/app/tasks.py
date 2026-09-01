"""Demo tasks -- each one exists to make a specific failure visible.

``slow_job``    a healthy long task: watch its terminal stream.
``stuck_job``   never returns: watch the watchdog take the process out.
``locked_job``  takes a lock and dies without releasing it: watch it strand.

Nothing here is Selenium; the library does not require it. The screenshot
feature is exercised by ``fake_frame``, which draws a PNG instead of driving a
browser -- the API is identical.
"""
from __future__ import annotations

import logging
import os
import random
import time
import zlib

from celery import Celery

import celery_liveops as liveops

BROKER = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672//")
REDIS = os.environ.get("LIVEOPS_REDIS_URL", "redis://redis:6379/0")

app = Celery("demo", broker=BROKER, backend=REDIS)
app.conf.update(
    task_acks_late=True,                # the watchdog's kill returns the message
    worker_prefetch_multiplier=1,       # a busy replica must not reserve work
    task_queues={"demo": {}, "forgotten": {}},   # "forgotten" has no consumer, on purpose
    task_default_queue="demo",
    task_routes={"demo.*": {"queue": "demo"}},
)

log = logging.getLogger("demo")
log.setLevel(logging.INFO)

#: Deliberately short so you can watch the watchdog fire without waiting.
DEADLINES = {"demo.slow_job": 300, "demo.stuck_job": 20, "demo.locked_job": 30}

liveops.install(
    app=app,
    logger=log,
    watchdog=DEADLINES,
    watchdog_enabled=os.environ.get("LIVEOPS_WATCHDOG_ENABLED", "true").lower() == "true",
    context_extractor=lambda name, args, kwargs: {"label": kwargs.get("label")},
)

liveops.register_lock(
    pattern="lock:import:*",
    label="Catalogue import (one per tenant)",
    module="import",
    blocks="New imports for that tenant exit immediately with 'already running', "
    "including the manual button. Nothing shows an error.",
)

RUNS_KEY = "demo:runs"


def remember(task_id: str, name: str) -> None:
    """Keep a short list of runs so the panel has something to show."""
    redis = liveops.redis_client()
    if redis is None:
        return
    redis.lpush(RUNS_KEY, f"{task_id}|{name}|{int(time.time())}")
    redis.ltrim(RUNS_KEY, 0, 24)


def fake_frame(step: int) -> bytes:
    """A tiny valid PNG, so the snapshot panel has something to draw.

    Stands in for ``driver.get_screenshot_as_png()``; `snapshot()` accepts raw
    bytes, a callable, or a WebDriver without caring which.
    """
    width = height = 64
    shade = (step * 37) % 256
    raw = b"".join(
        b"\x00" + bytes([shade, (shade + y) % 256, 200] * width) for y in range(height)
    )

    def chunk(kind: bytes, data: bytes) -> bytes:
        body = kind + data
        return (
            len(data).to_bytes(4, "big") + body + zlib.crc32(body).to_bytes(4, "big")
        )

    header = (width.to_bytes(4, "big") + height.to_bytes(4, "big")
              + bytes([8, 2, 0, 0, 0]))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", header)
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


@app.task(bind=True, name="demo.slow_job")
def slow_job(self, steps: int = 20, label: str = "import"):
    """A well-behaved long task: logs each step, stops itself before the deadline."""
    remember(self.request.id, "slow_job")

    with liveops.capture_logs(self.request.id):
        stop_by = liveops.safe_stop_at(task_name="demo.slow_job")
        log.info("starting %s: %s steps (stop by %s)", label, steps, stop_by.time())

        for step in range(1, steps + 1):
            import datetime

            if datetime.datetime.now() >= stop_by:
                # The cooperative stop: save a checkpoint and let the next run
                # resume, instead of being killed halfway through.
                log.warning("deadline approaching at step %s -- checkpointing", step)
                return {"stopped_at": step, "reason": "deadline"}

            time.sleep(1)
            log.info("step %s/%s ... %s", step, steps, random.choice(
                ["fetching", "parsing", "uploading", "waiting on the server"]
            ))
            liveops.snapshot(fake_frame(step))

        log.info("finished %s", label)
        return {"steps": steps}


@app.task(bind=True, name="demo.stuck_job")
def stuck_job(self, label: str = "hung"):
    """Never returns. With --pool=solo Celery's own time_limit would not fire."""
    remember(self.request.id, "stuck_job")

    with liveops.capture_logs(self.request.id):
        log.info("pretending to wait on a browser that will never answer")
        log.info("the watchdog deadline for this task is %ss",
                 liveops.deadline_for("demo.stuck_job"))
        while True:
            time.sleep(5)
            log.info("still hanging ...")


@app.task(bind=True, name="demo.locked_job")
def locked_job(self, tenant: str = "acme", die: bool = True):
    """Takes a lock, then dies without releasing it -- the orphan lock in the flesh."""
    remember(self.request.id, "locked_job")
    redis = liveops.redis_client()
    lock_key = f"lock:import:{tenant}"

    with liveops.capture_logs(self.request.id):
        if redis is not None and not redis.set(lock_key, self.request.id, nx=True, ex=600):
            # This is the silence the locks module exists to break: the task
            # "succeeds" and nothing ever runs.
            log.warning("already running for %s -- exiting", tenant)
            return {"skipped": True}

        log.info("took %s, importing for %s", lock_key, tenant)
        time.sleep(3)

        if die:
            log.error("process is about to die WITHOUT releasing the lock")
            os._exit(1)      # the finally never runs; the key stays behind

        redis.delete(lock_key)
        return {"tenant": tenant}
