"""Questions about the broker that a task table cannot answer.

Chiefly this one: **is anybody consuming that queue?** It is the quietest failure
a Celery deployment has. The message is published, the enqueue call returns
success, your screen says "queued" -- and nothing ever runs it, forever. It
happens the day a service is commented out of the compose file to free RAM, and
it is invisible until somebody asks why last Tuesday's report never arrived.

The two measurements here are deliberately different in how they fail:

- :func:`queue_depth` returns ``None`` when the broker will not answer. ``None``
  is **not** zero. A reaper deciding whether a pending item was abandoned must
  be able to tell "the queue is empty, so nobody is coming" from "I could not
  ask" -- otherwise it invents failures that never happened.
- :func:`has_consumer` returns ``True`` when it cannot tell. Callers use it to
  fail fast *before* enqueueing, and refusing a legitimate request because the
  broker was slow is worse than the wait.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from .config import settings

logger = logging.getLogger(__name__)

_app = None

#: Declared queues that are *expected* to have no consumer -- an off-by-design
#: service whose absence is already handled elsewhere. Keeps the orphan alert
#: honest instead of crying wolf every deploy.
EXPECTED_WITHOUT_CONSUMER: tuple = ()


def bind_app(app) -> None:
    """Point this module at your Celery app.

    Optional: without it, ``celery.current_app`` is used. Bind explicitly when
    your process holds more than one app, or when import order makes
    ``current_app`` resolve to the wrong one.
    """
    global _app
    _app = app


def _celery_app():
    if _app is not None:
        return _app
    try:
        from celery import current_app

        return current_app
    except ImportError:
        return None


def queue_depth(queue: str) -> Optional[int]:
    """How many messages are *waiting* in the queue. ``None`` when unknown.

    A passive ``queue_declare``, the same call the management UI makes. Requires
    an AMQP broker (RabbitMQ); with a Redis broker it returns ``None``.

    One channel, closed at the end: passively declaring a queue that does not
    exist closes the channel, and reusing it would silently make the next answer
    zero.
    """
    app = _celery_app()
    if app is None:
        return None
    try:
        with app.pool.acquire(block=True) as conn:
            channel = conn.channel()
            try:
                declared = channel.queue_declare(queue=queue, passive=True)
                return int(declared.message_count)
            finally:
                try:
                    channel.close()
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("celery-liveops: measuring queue %s failed: %s", queue, exc)
        return None


def consumers_by_queue(timeout: Optional[float] = None) -> Dict[str, List[str]]:
    """``{queue: [nodes consuming it]}``, in a single sweep of the cluster.

    One ``inspect`` call for every queue, not one per queue: each ``inspect`` is
    a broadcast on the broker, and an endpoint under polling would multiply that
    traffic by the number of queues you have.

    Returns ``{}`` when nobody answers -- the caller decides what that means.
    """
    app = _celery_app()
    if app is None:
        return {}
    timeout = settings().inspect_timeout if timeout is None else timeout
    try:
        by_node = app.control.inspect(timeout=timeout).active_queues() or {}
    except Exception as exc:
        logger.warning("celery-liveops: inspecting worker queues failed: %s", exc)
        return {}

    mapping: Dict[str, List[str]] = {}
    for node, queues in by_node.items():
        for q in queues or []:
            name = (q or {}).get("name")
            if name:
                mapping.setdefault(name, []).append(node)
    return mapping


def has_consumer(queue: str, timeout: Optional[float] = None) -> bool:
    """Is any live worker consuming ``queue``?

    **Optimistically best-effort**: returns ``True`` when the inspect fails or
    times out. See the module docstring for why that asymmetry is deliberate.
    """
    app = _celery_app()
    if app is None:
        return True
    timeout = settings().inspect_timeout if timeout is None else timeout
    try:
        by_node = app.control.inspect(timeout=timeout).active_queues() or {}
    except Exception as exc:
        logger.warning(
            "celery-liveops: inspect failed, assuming a consumer for %s: %s", queue, exc
        )
        return True
    for queues in by_node.values():
        if any((q or {}).get("name") == queue for q in (queues or [])):
            return True
    return False


def orphan_queues(timeout: Optional[float] = None) -> List[str]:
    """Declared queues that no worker is consuming.

    Returns ``[]`` when the inspect does not answer: with no measurement you
    accuse nothing, otherwise the alert fires on every broker blip.
    """
    app = _celery_app()
    if app is None:
        return []
    try:
        declared = list(app.conf.task_queues or {})
    except Exception as exc:
        logger.warning("celery-liveops: reading declared queues failed: %s", exc)
        return []

    mapping = consumers_by_queue(timeout=timeout)
    if not mapping:
        return []
    return [q for q in declared if q not in mapping and q not in EXPECTED_WITHOUT_CONSUMER]
