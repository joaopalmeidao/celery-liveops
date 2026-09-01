"""Presence: who is alive *right now*.

Two questions a task table cannot answer on its own:

1. **Which workers are up?** Not "which ones were up when the page loaded" --
   which ones are answering this second.
2. **Does this row still have somebody on the other end?** A run whose process
   died (deploy, OOM, a watchdog calling ``os._exit``, a container restart) looks
   exactly like a healthy long-running one: still marked started, never finished.
   And because Celery **keeps the task id across retries**, the next attempt
   would show up as a second card sharing the dead one's terminal.

The answer comes from Celery's own signals. ``worker_ready`` announces the
process and starts one daemon thread that keeps its heartbeat warm for as long
as it lives -- an idle worker still has to answer "are you up?" -- and
``task_prerun``/``task_postrun`` add and remove the current run's own key.
Readers only ever do ``EXISTS`` -- no ``celery inspect``, which is slow and times
out exactly when you need it, in an endpoint being polled every few seconds.

Every key carries a TTL, so a process dying is self-cleaning: nothing is left
behind claiming to be alive.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from .config import settings
from .store import client, guard
from .store import key as _k

logger = logging.getLogger(__name__)

ContextExtractor = Callable[[Optional[str], tuple, dict], Dict[str, Any]]

_installed = False
_lock = threading.Lock()
_context_extractor: Optional[ContextExtractor] = None

# One refresh thread per PROCESS, not per task. It keeps the worker's heartbeat
# warm whether or not anything is running -- an idle worker still has to answer
# "are you up?" -- and refreshes the current run's presence when there is one.
_stop: Optional[threading.Event] = None
_thread: Optional[threading.Thread] = None
_current: dict = {"task_id": None, "context": {}}


def worker_id() -> str:
    """Identity of this executor: ``hostname:pid``.

    Resolved on every call rather than cached at import. In a prefork worker this
    module is imported in the parent, before the fork -- a value frozen there
    would have every child writing to the same key and overwriting each other,
    and your panel would show one worker instead of eight.
    """
    return f"{socket.gethostname()}:{os.getpid()}"


def _alive_key(task_id: str) -> str:
    return _k("alive", task_id)


def _worker_key(wid: str) -> str:
    return _k("worker", wid)


def _worker_task_key(wid: str) -> str:
    return _k("worker", wid, "task")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Writing (worker side) ────────────────────────────────────────────────────


def heartbeat(status: str = "idle") -> None:
    """Announce that this worker process is alive."""
    redis = client()
    if redis is None:
        return
    conf = settings()
    with guard("heartbeat"):
        redis.set(
            _worker_key(worker_id()),
            json.dumps(
                {
                    "worker_id": worker_id(),
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                    "status": status,
                    "last_seen": _now(),
                }
            ),
            ex=conf.worker_ttl,
        )


def mark_alive(task_id, context: Optional[Dict[str, Any]] = None) -> None:
    """Write (or refresh) presence for one run. Best-effort, never raises."""
    if not task_id:
        return
    redis = client()
    if redis is None:
        return
    conf = settings()
    with guard("mark_alive"):
        payload = {"worker_id": worker_id(), **(context or {})}
        redis.set(_alive_key(task_id), json.dumps(payload), ex=conf.presence_ttl)


def clear_alive(task_id) -> None:
    """Drop a run's presence key."""
    if not task_id:
        return
    redis = client()
    if redis is None:
        return
    with guard("clear_alive"):
        redis.delete(_alive_key(task_id))


# ── Reading (API side) ───────────────────────────────────────────────────────


def is_alive(task_id) -> bool:
    """Is this run still being executed by a living process?"""
    if not task_id:
        return False
    redis = client()
    if redis is None:
        return False
    try:
        return bool(redis.exists(_alive_key(task_id)))
    except Exception:
        return False


def alive_among(task_ids: Iterable[str]) -> Set[str]:
    """Which of ``task_ids`` still have presence -- in a single round trip.

    Returns an empty set when Redis fails. Callers must read that as "no signal",
    never as an error to show the user: a Redis blip should grey out a badge, not
    break the page.
    """
    ids = [t for t in dict.fromkeys(task_ids or []) if t]
    if not ids:
        return set()
    redis = client()
    if redis is None:
        return set()
    try:
        pipe = redis.pipeline()
        for tid in ids:
            pipe.exists(_alive_key(tid))
        return {tid for tid, found in zip(ids, pipe.execute()) if found}
    except Exception:
        return set()


def workers() -> List[dict]:
    """Every worker process that has sent a heartbeat recently."""
    redis = client()
    if redis is None:
        return []
    prefix = _worker_key("")
    found: List[dict] = []
    try:
        for wkey in redis.scan_iter(match=f"{prefix}*", count=100):
            # The worker id is "hostname:pid" and therefore *contains* a colon:
            # slice the prefix off, never split on ":" and take the last part.
            if wkey.endswith(":task"):
                continue
            wid = wkey[len(prefix):]
            raw = redis.get(wkey)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (TypeError, ValueError):
                continue
            task_raw = redis.get(_worker_task_key(wid))
            data["current_task"] = json.loads(task_raw) if task_raw else None
            found.append(data)
    except Exception as exc:
        logger.debug("celery-liveops: listing workers failed: %s", exc)
        return found
    found.sort(key=lambda w: w.get("worker_id", ""))
    return found


# ── Celery wiring ────────────────────────────────────────────────────────────


def _refresh_forever(stop: threading.Event) -> None:
    """Keep this process's presence warm (daemon thread).

    Runs for the life of the worker, not the life of a task: a worker with
    nothing to do still has to answer "are you up?", and a panel that only lists
    busy workers reports zero the moment the queue drains.
    """
    conf = settings()
    while not stop.wait(conf.presence_refresh):
        try:
            task_id = _current["task_id"]
            heartbeat("busy" if task_id else "idle")
            if task_id:
                mark_alive(task_id, _current["context"])
                redis = client()
                if redis is not None:
                    redis.expire(_worker_task_key(worker_id()), conf.worker_ttl)
        except Exception:
            # Redis being down must not take the worker with it. Worst case the
            # run shows as "no signal" and your reaper closes it.
            pass


def _ensure_refresher() -> None:
    """Start the process's refresh thread once."""
    global _stop, _thread

    with _lock:
        if _thread is not None and _thread.is_alive():
            return
        _stop = threading.Event()
        _thread = threading.Thread(
            target=_refresh_forever, args=(_stop,), daemon=True, name="liveops-presence"
        )
        _thread.start()


def _on_worker_ready(**_):
    """Announce an idle worker the moment it is ready to consume."""
    heartbeat("idle")
    _ensure_refresher()


def _on_worker_shutdown(**_):
    """Stop claiming to be up. The TTL would do it, but not for another minute."""
    global _stop, _thread

    with _lock:
        if _stop is not None:
            _stop.set()
        _stop = None
        _thread = None

    redis = client()
    if redis is not None:
        with guard("presence shutdown"):
            redis.delete(_worker_key(worker_id()), _worker_task_key(worker_id()))


def _start(task_id=None, task=None, args=None, kwargs=None, **_):
    task_name = getattr(task, "name", None)
    context: Dict[str, Any] = {"task_name": task_name, "started_at": _now()}
    if _context_extractor is not None:
        try:
            context.update(_context_extractor(task_name, args or (), kwargs or {}) or {})
        except Exception as exc:
            logger.debug("celery-liveops: context extractor failed: %s", exc)

    _current["task_id"] = task_id
    _current["context"] = context

    redis = client()
    if redis is not None:
        with guard("presence start"):
            redis.set(
                _worker_task_key(worker_id()),
                json.dumps({"task_id": task_id, **context}),
                # Short TTL (a few times the refresh interval): if the process
                # dies the key vanishes on its own instead of claiming the worker
                # is busy for the next hour.
                ex=settings().worker_ttl,
            )
    mark_alive(task_id, context)
    heartbeat("busy")
    # Also covers the eager/embedded case, where worker_ready never fires.
    _ensure_refresher()


def _finish(task_id=None, **_):
    _current["task_id"] = None
    _current["context"] = {}

    clear_alive(task_id)
    redis = client()
    if redis is not None:
        with guard("presence finish"):
            redis.delete(_worker_task_key(worker_id()))
    heartbeat("idle")


def install_presence(context_extractor: Optional[ContextExtractor] = None) -> bool:
    """Connect presence tracking to Celery's task signals.

    Call it once, in the module your worker imports (``celeryconfig``, your app
    factory, anywhere that runs in the worker process).

    ``context_extractor`` adds your own labels to what the panel shows -- tenant,
    document, region -- from the task's name and arguments::

        def label(task_name, args, kwargs):
            return {"tenant": kwargs.get("tenant_id")}

        install_presence(context_extractor=label)

    Returns ``False`` when Celery is not installed, so importing this library in
    a web-only process is harmless.
    """
    global _installed, _context_extractor

    _context_extractor = context_extractor
    if _installed:
        return True
    try:
        from celery.signals import (
            task_postrun,
            task_prerun,
            worker_ready,
            worker_shutdown,
        )
    except ImportError:
        logger.debug("celery-liveops: celery not installed, presence not wired")
        return False

    task_prerun.connect(_start, weak=False)
    task_postrun.connect(_finish, weak=False)
    # Idle workers count: without these two, the panel lists a worker only while
    # it happens to be busy, and reports zero as soon as the queue drains.
    worker_ready.connect(_on_worker_ready, weak=False)
    worker_shutdown.connect(_on_worker_shutdown, weak=False)
    _installed = True
    return True
