"""A hard deadline per task -- the ``time_limit`` Celery does not give you on
every pool.

Celery enforces ``task_time_limit`` by killing the *child* process of a prefork
pool. With ``--pool=solo`` there is no child, so the limit never fires. A solo
worker stuck on one task (a hung browser, a retry storm, a socket that never
times out) holds its queue until somebody restarts it by hand.

This module arms a daemon :class:`threading.Timer` when a task starts. If the
deadline passes, the process terminates itself. With ``restart: unless-stopped``
the container comes back clean and, because ``task_acks_late=True``, the message
returns to the queue instead of being lost. During a blocked network call the
timer thread keeps running -- socket I/O releases the GIL -- so the shot lands.

**The deadline belongs to the task, not to the container.** That distinction is
the whole point. When the timeout is an environment variable, every duration
profile needs its own service: 15 minutes for the short job, 8 hours for the
overnight one, and a pool of identical replicas can never serve both -- the long
run dies halfway through in the short container. Registered per task name, one
fungible pool serves every profile.

Two things to know before enabling it:

- The task's ``finally`` does **not** run. Whatever bookkeeping row you opened
  stays open, and something else (a reaper) has to close it. That is the price of
  killing a process that is not answering.
- Cooperative stopping is better whenever the task can manage it. Have long
  loops check :func:`safe_stop_at`, write a checkpoint and exit cleanly; the
  watchdog is then only the backstop for code that cannot.
"""
from __future__ import annotations

import datetime
import logging
import os
import threading
from typing import Callable, Dict, Optional

from .config import settings

logger = logging.getLogger(__name__)

#: Deadline in seconds, by task name. Anything not listed falls back to
#: ``settings().default_deadline``.
_deadlines: Dict[str, int] = {}

_timer: Optional[threading.Timer] = None
_lock = threading.Lock()
_installed = False
_on_timeout: Optional[Callable[[str, str, int], None]] = None
_enabled: Optional[bool] = None


def set_deadline(task_name: str, seconds: int) -> None:
    """Register the deadline of one task."""
    _deadlines[task_name] = int(seconds)


def set_deadlines(mapping: Dict[str, int]) -> None:
    """Register several at once (does not clear existing entries)."""
    for name, seconds in (mapping or {}).items():
        set_deadline(name, seconds)


def deadlines() -> Dict[str, int]:
    """A copy of the registry."""
    return dict(_deadlines)


def deadline_for(task_name: Optional[str] = None) -> int:
    """Deadline in seconds for ``task_name``, or the configured default.

    Called with no argument it keeps the old meaning of a process-wide timeout,
    which is the fallback for every unregistered task.
    """
    if task_name and task_name in _deadlines:
        return _deadlines[task_name]
    return settings().default_deadline


def safe_stop_at(
    started_at: Optional[datetime.datetime] = None,
    task_name: Optional[str] = None,
) -> datetime.datetime:
    """The moment a cooperative task should stop itself to avoid being killed.

    The default 10% margin pays for teardown -- closing a browser, writing a
    checkpoint, marking the run finished. Compare against it inside your loop::

        stop_by = safe_stop_at(task_name="crawl")
        for page in pages:
            if datetime.datetime.now() >= stop_by:
                save_checkpoint(page)
                break
    """
    now = started_at or datetime.datetime.now()
    budget = deadline_for(task_name)
    margin = max(60, int(budget * settings().safe_fraction))
    return now + datetime.timedelta(seconds=margin)


def _terminate(task_name: str, task_id: str, timeout: int) -> None:
    logger.critical(
        "[liveops watchdog] task %s[%s] exceeded %ss and is presumed hung. "
        "Terminating the process to free the queue; the message returns to the "
        "broker via acks_late.",
        task_name,
        task_id,
        timeout,
    )
    if _on_timeout is not None:
        try:
            _on_timeout(task_name, task_id, timeout)
        except Exception as exc:  # pragma: no cover - hook is user code
            logger.error("[liveops watchdog] on_timeout hook failed: %s", exc)

    # os._exit ends the process immediately from the timer thread, without
    # running finalizers and without depending on signal delivery -- portable,
    # since signal.SIGKILL does not exist on Windows. The task is wedged and
    # would not answer a graceful shutdown anyway; a container restart clears
    # whatever state it was holding.
    os._exit(1)


def _arm(task_id=None, task=None, **_):
    if not _is_enabled():
        return
    global _timer
    with _lock:
        if _timer is not None:
            _timer.cancel()
        task_name = getattr(task, "name", "?")
        timeout = deadline_for(task_name)
        _timer = threading.Timer(timeout, _terminate, args=(task_name, task_id, timeout))
        _timer.daemon = True
        _timer.start()


def _disarm(**_):
    if not _is_enabled():
        return
    global _timer
    with _lock:
        if _timer is not None:
            _timer.cancel()
            _timer = None


def _is_enabled() -> bool:
    return settings().watchdog_enabled if _enabled is None else _enabled


def install_watchdog(
    deadlines: Optional[Dict[str, int]] = None,
    enabled: Optional[bool] = None,
    on_timeout: Optional[Callable[[str, str, int], None]] = None,
) -> bool:
    """Arm the per-task watchdog on Celery's task signals.

    Disabled unless ``enabled=True`` or ``LIVEOPS_WATCHDOG_ENABLED=true``: a
    library that kills processes has to be opted into, never switched on by the
    act of installing it. Leave it off in your API, beat and flower processes.

    ``on_timeout(task_name, task_id, timeout)`` runs just before the process
    dies -- your chance to emit a metric or mark the row. Keep it short and
    non-blocking; it is running inside a process that is about to be gone.

    A single global timer is enough even with a prefork pool: the signals fire
    *inside* the child process, so each child has its own.
    """
    global _installed, _on_timeout, _enabled

    if deadlines:
        set_deadlines(deadlines)
    if enabled is not None:
        _enabled = bool(enabled)
    _on_timeout = on_timeout

    if _installed:
        return True
    try:
        from celery.signals import task_postrun, task_prerun
    except ImportError:
        logger.debug("celery-liveops: celery not installed, watchdog not wired")
        return False

    task_prerun.connect(_arm, weak=False)
    task_postrun.connect(_disarm, weak=False)
    _installed = True

    if _is_enabled():
        logger.info(
            "[liveops watchdog] armed: %s task(s) with their own deadline, "
            "%ss fallback for the rest.",
            len(_deadlines),
            settings().default_deadline,
        )
    return True
