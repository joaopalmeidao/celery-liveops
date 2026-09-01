"""Resizing a running worker, and making that size survive a restart.

With ``--pool=prefork`` Celery can grow and shrink its pool at runtime, so an
operator can add executors during a backlog without a redeploy. With
``--pool=solo`` there is no pool to grow: the broadcast is *accepted and does
nothing*. That is the trap this module exists to close -- :func:`apply_target`
refuses up front instead of returning success and changing nothing.

When the worker runs with ``--autoscale``, demand sizes the pool and the number
you pick means the **ceiling**, applied through the autoscaler rather than
``pool_grow``. Growing the pool directly there would be undone at the first idle
moment.

The ceiling itself is not a formality. Each executor of a browser worker is a
Chrome process at roughly a gigabyte; a mistyped number is an out-of-memory kill
you requested in advance.
"""
from __future__ import annotations

import logging
import os
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

#: Absolute ceiling, whatever anyone types. Override per service with the
#: ``LIVEOPS_MAX_CONCURRENCY`` environment variable.
DEFAULT_MAX_CONCURRENCY = 12


def max_concurrency() -> int:
    """This process's ceiling on executors."""
    try:
        value = int(os.environ.get("LIVEOPS_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY))
    except (TypeError, ValueError):
        value = DEFAULT_MAX_CONCURRENCY
    return max(1, value)


def queues_of_process() -> List[str]:
    """Queues this worker consumes, read from ``CELERY_QUEUES``.

    Your boot script already passes them to ``celery worker -Q``; putting the
    same value in the environment is what lets the process know its own identity
    without parsing its command line.
    """
    raw = os.environ.get("CELERY_QUEUES") or ""
    return [q.strip() for q in raw.split(",") if q.strip()]


def pool_size(sender) -> Optional[int]:
    """Current number of executors, or ``None`` for a pool that has none (solo)."""
    pool = getattr(sender, "pool", None)
    return getattr(pool, "num_processes", None)


def set_autoscale_ceiling(sender, target: int) -> bool:
    """Move this worker's autoscaler ceiling to ``target``. ``True`` if it moved.

    ``Autoscaler.update`` shrinks immediately when the live pool is above the new
    maximum, so this both raises and lowers.
    """
    autoscaler = getattr(sender, "autoscaler", None)
    if not autoscaler:
        return False
    try:
        maximum, minimum = autoscaler.update(max=int(target))
        logger.info("celery-liveops: autoscaler ceiling set to max=%s min=%s", maximum, minimum)
        return True
    except Exception as exc:
        logger.warning("celery-liveops: setting autoscaler ceiling to %s failed: %s", target, exc)
        return False


def resize_pool(sender, target: int, current: Optional[int] = None) -> bool:
    """Grow or shrink this worker's pool to ``target``. ``True`` if it moved."""
    current = pool_size(sender) if current is None else current
    if not current:
        return False
    delta = int(target) - int(current)
    if not delta:
        return False
    try:
        if delta > 0:
            sender.pool.grow(delta)
        else:
            sender.pool.shrink(-delta)
        logger.info("celery-liveops: pool resized from %s to %s executor(s)", current, target)
        return True
    except Exception as exc:
        logger.warning("celery-liveops: resizing pool to %s failed: %s", target, exc)
        return False


def apply_target(sender, target: int) -> bool:
    """Apply a desired executor count to this worker.

    Prefers the autoscaler ceiling when there is one, falls back to resizing the
    pool. Returns ``False`` on a solo pool -- there is nothing to resize, and
    saying otherwise would be lying to whoever pressed the button.
    """
    current = pool_size(sender)
    if not current:
        logger.info("celery-liveops: solo pool has no executors to resize; target ignored")
        return False
    target = max(1, min(int(target), max_concurrency()))
    if set_autoscale_ceiling(sender, target):
        return True
    return resize_pool(sender, target, current)


TargetLoader = Callable[[List[str]], Optional[int]]


def install_boot_scale(loader: TargetLoader) -> bool:
    """Re-apply a persisted executor count when the worker boots.

    Without this, any restart -- deploy, OOM, watchdog -- silently reverts to the
    concurrency baked into the environment, while your screen keeps showing the
    number the operator chose. ``loader`` receives this process's queues and
    returns the desired count, or ``None`` to leave it alone::

        install_boot_scale(lambda queues: db.get_target(queues[0]))

    Best-effort: if the loader raises, the worker keeps the environment's value.
    It never prevents a worker from starting.
    """
    try:
        from celery.signals import worker_ready
    except ImportError:
        logger.debug("celery-liveops: celery not installed, boot scaling not wired")
        return False

    def _on_ready(sender=None, **_):
        try:
            if not pool_size(sender):
                return  # solo pool: nothing to grow
            target = loader(queues_of_process())
            if target is None:
                return
            apply_target(sender, target)
        except Exception as exc:
            logger.warning("celery-liveops: applying persisted scale at boot failed: %s", exc)

    worker_ready.connect(_on_ready, weak=False)
    return True
