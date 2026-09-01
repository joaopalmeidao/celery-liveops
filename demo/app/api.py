"""The panel's backend: the library's own router, plus three buttons to start work.

Note what is *not* here. There is no database, no task table, no bookkeeping of
any kind -- every number on the page comes from Redis and the broker. In a real
system you would have your own runs table; the point of the demo is that the
live half does not need one.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import celery_liveops as liveops
from celery_liveops.contrib.fastapi import liveops_router

from .tasks import RUNS_KEY, locked_job, slow_job, stuck_job
from .tasks import app as celery_app

STATIC = Path(__file__).resolve().parent.parent / "static"

api = FastAPI(title="celery-liveops demo")

liveops.bind_app(celery_app)
liveops.register_lock(
    pattern="lock:import:*",
    label="Catalogue import (one per tenant)",
    module="import",
    blocks="New imports for that tenant exit immediately with 'already running'.",
)

# public=True only because this demo runs on your laptop. In a real app you pass
# dependencies=[Depends(your_auth)] -- the router refuses to build without one.
api.include_router(liveops_router(public=True))


@api.post("/demo/start/{kind}")
def start(kind: str, steps: int = 20):
    """Queue one of the demo tasks."""
    task = {"slow": slow_job, "stuck": stuck_job, "locked": locked_job}[kind]
    result = task.delay(steps=steps) if kind == "slow" else task.delay()
    return {"task_id": result.id, "kind": kind}


@api.get("/demo/runs")
def runs():
    """Recent runs, annotated with whether anybody is still executing them."""
    redis = liveops.redis_client()
    rows = []
    raw = redis.lrange(RUNS_KEY, 0, -1) if redis is not None else []
    for item in raw:
        task_id, name, started = item.split("|")
        rows.append({"task_id": task_id, "name": name, "started": int(started)})

    alive = liveops.alive_among([r["task_id"] for r in rows])
    for row in rows:
        row["alive"] = row["task_id"] in alive
    return {"runs": rows}


@api.get("/demo/overview")
def overview():
    """Everything the header needs, in one call."""
    return {
        "workers": liveops.workers(),
        "queue_depth": liveops.queue_depth("demo"),
        "orphan_queues": liveops.orphan_queues(),
        "consumers": liveops.consumers_by_queue(),
        "snapshot_stats": liveops.snapshot_stats(),
    }


api.mount("/static", StaticFiles(directory=STATIC), name="static")


@api.get("/")
def index():
    return FileResponse(STATIC / "index.html")
