"""Redis access, with the one property the rest of the library depends on:
**it never raises**.

Everything here is diagnostics. Live logs, presence and screenshots exist so a
human can see what a long task is doing; none of them is on the critical path of
the work itself. A Redis outage must therefore degrade the panel, never the job.
So `client()` returns ``None`` instead of raising, and `guard()` swallows.

The client is created lazily rather than at import, for a reason that costs real
incidents when ignored: in a prefork worker this module is imported in the parent,
before the fork. A connection opened at import time is inherited by every child,
and children then share sockets.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Optional

from . import config

logger = logging.getLogger(__name__)

_lazy_client: Any = None
_lazy_url: Optional[str] = None


def client() -> Optional[Any]:
    """A Redis client, or ``None`` when one cannot be had.

    Resolution order: the client injected via :func:`celery_liveops.configure`,
    then the injected factory, then a connection built from ``redis_url``.
    """
    global _lazy_client, _lazy_url

    if config._redis_client is not None:
        return config._redis_client

    if config._redis_factory is not None:
        try:
            return config._redis_factory()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("celery-liveops: redis factory failed: %s", exc)
            return None

    url = config.settings().redis_url
    # Rebuild when the URL changed under us (a `configure()` call in a test).
    if _lazy_client is None or _lazy_url != url:
        try:
            import redis
        except ImportError:  # pragma: no cover - redis is a hard dependency
            logger.warning("celery-liveops: the 'redis' package is not installed")
            return None
        try:
            _lazy_client = redis.from_url(url, decode_responses=True)
            _lazy_url = url
        except Exception as exc:
            logger.warning("celery-liveops: cannot connect to %s: %s", url, exc)
            return None
    return _lazy_client


def key(*parts: Any) -> str:
    """Namespaced Redis key: ``{key_prefix}:part:part``."""
    return ":".join([config.settings().key_prefix, *(str(p) for p in parts)])


@contextmanager
def guard(what: str = ""):
    """Run a Redis interaction, swallowing every failure.

    Logged at DEBUG, not WARNING: when Redis is down this fires on every log
    line, and a diagnostics channel that floods the very logs it is trying to
    capture is worse than a silent one.
    """
    try:
        yield
    except Exception as exc:
        logger.debug("celery-liveops: %s failed: %s", what or "redis call", exc)


def reset_client() -> None:
    """Drop the lazily built client (tests, or after a fork)."""
    global _lazy_client, _lazy_url
    _lazy_client = None
    _lazy_url = None
