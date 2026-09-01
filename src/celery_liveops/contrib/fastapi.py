"""A ready-made FastAPI router exposing the read side of celery-liveops.

Mount it and you have the panel's backend::

    from fastapi import Depends, FastAPI
    from celery_liveops.contrib.fastapi import liveops_router

    app = FastAPI()
    app.include_router(liveops_router(dependencies=[Depends(require_operator)]))

**Authentication is your job and this router will not let you forget it.** It
exposes a task's log output, a screenshot of what it is looking at, and a
``DELETE`` on infrastructure locks -- so ``liveops_router()`` refuses to build
without either ``dependencies=[...]`` or an explicit ``public=True``. Releasing a
lock is still guarded by the allowlist in :mod:`celery_liveops.locks`; the raw
key travels from the browser, and without that check an arbitrary ``DEL`` against
production Redis would be one POST away.
"""
from __future__ import annotations

from typing import List, Optional, Sequence

try:
    from fastapi import APIRouter, Body, HTTPException
except ImportError as exc:  # pragma: no cover - optional dependency
    raise ImportError(
        "celery_liveops.contrib.fastapi requires fastapi: pip install 'celery-liveops[fastapi]'"
    ) from exc

from .. import locks as _locks
from .. import presence as _presence
from .. import queues as _queues
from .. import snapshots as _snapshots
from ..logs import read_any_logs


def liveops_router(
    prefix: str = "/liveops",
    tags: Optional[Sequence[str]] = None,
    dependencies: Optional[Sequence] = None,
    public: bool = False,
    allow_release: bool = True,
) -> APIRouter:
    """Build the router.

    ``allow_release=False`` keeps everything read-only, which is the right
    setting for a dashboard that anyone on the team can open.
    """
    if not dependencies and not public:
        raise RuntimeError(
            "liveops_router exposes task logs, screenshots and lock release. "
            "Pass dependencies=[Depends(your_auth)], or public=True if you have "
            "already put authentication in front of this app."
        )

    router = APIRouter(
        prefix=prefix,
        tags=list(tags) if tags else ["liveops"],
        dependencies=list(dependencies) if dependencies else None,
    )

    @router.get("/runs/{task_id}/logs")
    def get_logs(task_id: str) -> dict:
        """The run's terminal: the live buffer, or the archived previous attempt."""
        return {
            "task_id": task_id,
            "alive": _presence.is_alive(task_id),
            "logs": read_any_logs(task_id),
        }

    @router.get("/runs/{task_id}/snapshot")
    def get_snapshot(task_id: str) -> dict:
        """The latest frame captured for this run, base64-encoded PNG."""
        image = _snapshots.read_snapshot(task_id)
        if image is None:
            raise HTTPException(status_code=404, detail="No snapshot for this run.")
        return {"task_id": task_id, "image_base64": image}

    @router.post("/runs/alive")
    def post_alive(task_ids: List[str] = Body(..., embed=True)) -> dict:
        """Which of these runs still have a living process behind them.

        A POST because a live table asks about a page of ids at once, and one
        round trip beats one request per row.
        """
        return {"alive": sorted(_presence.alive_among(task_ids))}

    @router.get("/workers")
    def get_workers() -> dict:
        """Worker processes that have sent a heartbeat recently."""
        return {"workers": _presence.workers()}

    @router.get("/locks")
    def get_locks() -> dict:
        """Registered locks currently held, with their remaining TTL."""
        return {"locks": _locks.list_locks()}

    if allow_release:

        @router.post("/locks/release")
        def post_release(keys: List[str] = Body(..., embed=True)) -> dict:
            """Release held locks. Keys outside the catalogue are refused, not fatal."""
            return _locks.release_locks(keys)

    @router.get("/queues")
    def get_queues() -> dict:
        """Who consumes what, and which declared queue nobody consumes."""
        consumers = _queues.consumers_by_queue()
        return {
            "consumers": consumers,
            "orphans": _queues.orphan_queues(),
        }

    @router.get("/queues/{queue}/depth")
    def get_depth(queue: str) -> dict:
        """Messages waiting in a queue. ``null`` means the broker did not answer -- not zero."""
        return {"queue": queue, "depth": _queues.queue_depth(queue)}

    return router
