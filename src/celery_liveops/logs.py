"""The live terminal: every log line a task emits, readable from another process
while the task is still running.

The transport is Redis because the two ends live in different containers -- the
worker writes, your API reads -- and because a capped list (``RPUSH`` + ``LTRIM``)
is exactly the data structure a scrolling terminal wants.

Attachment is a single :class:`LiveLogHandler` on your logger. It is a cheap
no-op whenever no run is active in the :class:`~contextvars.ContextVar`, so the
same logging configuration serves your API, your beat process and your workers.

One behaviour is worth reading before you change it: **the previous attempt's
buffer is archived, not deleted** (see :func:`capture_logs`).
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional, Tuple

from .config import settings
from .store import client, guard
from .store import key as _k

#: Key of the run being captured in this context. Set per task.
_current: ContextVar = ContextVar("liveops_current_key", default=None)
#: Additional keys that receive a copy of the same lines (see `capture_logs`).
#: They live in their own ContextVar so that the *root* -- the only key that
#: cancellation and live screenshots look at -- stays the task's own id.
_extra: ContextVar = ContextVar("liveops_extra_keys", default=())

_ORPHAN_SUFFIX = "orphan"


def _log_key(run_key: str) -> str:
    return _k("log", run_key)


def _orphan_key(run_key: str) -> str:
    return _k("log", run_key, _ORPHAN_SUFFIX)


def current_key() -> Optional[str]:
    """Key of the run active in this context, or ``None`` outside a task.

    This is the thread that lets code at the bottom of the stack -- a driver, a
    scraper, a retry helper -- discover which run it belongs to without having
    the id threaded through every signature. Cooperative cancellation is built
    on exactly this.

    It always returns the *root* key, never an extra one: code down there needs
    the real task id, because that is what a stop request is keyed on.
    """
    return _current.get()


class LiveLogHandler(logging.Handler):
    """Pushes formatted log records into the active run's Redis buffer.

    Resilient by construction: a no-op with no active run, never raises, and caps
    both line length and line count so a chatty task cannot grow without bound.
    """

    def emit(self, record: logging.LogRecord) -> None:
        keys = [k for k in (_current.get(), *_extra.get()) if k]
        if not keys:
            return
        conf = settings()
        try:
            msg = self.format(record)
            if len(msg) > conf.max_line_length:
                msg = msg[: conf.max_line_length] + " ...[line truncated]"
            redis = client()
            if redis is None:
                return
            pipe = redis.pipeline()
            for run_key in keys:
                rk = _log_key(run_key)
                pipe.rpush(rk, msg)
                pipe.ltrim(rk, -conf.max_lines, -1)  # keep only the last N lines
                pipe.expire(rk, conf.log_ttl)
            pipe.execute()
        except Exception:
            # Logging must never break the task it is observing.
            pass


@contextmanager
def capture_logs(run_key, extra: bool = False) -> Iterator[None]:
    """Capture every log line emitted inside the block into ``run_key``.

    A falsy key is a no-op, so the same wrapper works for code paths that run
    both inside and outside a task.

    ``extra=True`` *stacks* the key instead of replacing the root: the same lines
    are written to both buffers. That is what lets a task processing a batch keep
    one terminal for the whole batch while also archiving each item's slice under
    its own key. The root is deliberately left alone, so `current_key()` and live
    snapshots keep pointing at the real task id.
    """
    if not run_key:
        yield
        return

    _archive_previous_attempt(run_key)

    if extra:
        var, value = _extra, (*_extra.get(), str(run_key))
    else:
        var, value = _current, str(run_key)
    token = var.set(value)
    try:
        yield
    finally:
        var.reset(token)


def _archive_previous_attempt(run_key) -> None:
    """Move whatever the previous attempt left behind into the orphan key.

    Celery **keeps the task id across retries**. When a process dies without
    running its ``finally`` -- OOM, a watchdog calling ``os._exit``, a deploy
    mid-run -- that attempt's log exists nowhere but Redis. The next attempt used
    to start by deleting it, destroying the only evidence of why the first one
    died.

    So it is renamed instead, and your reaper can read it (`read_orphan_logs`)
    and archive it against the failed run before closing that row.

    Best-effort, and it always leaves the main key empty -- that is the contract
    of `capture_logs`. If the RENAME fails we fall back to the DELETE it
    replaced: interleaving two attempts' logs is worse than losing one.

    The common case (nothing to preserve) costs a single call, same as the
    DELETE that used to live here. That is not micro-optimisation: with Redis
    unreachable every call costs the client's full timeout, and a fallback that
    repeats the operation would double that wait on every single task.
    """
    if not run_key:
        return
    main = _log_key(run_key)
    redis = client()
    if redis is None:
        return
    try:
        if not redis.exists(main):
            return
    except Exception:
        return  # Redis is down: nothing to preserve and nothing to delete

    try:
        orphan = _orphan_key(run_key)
        redis.rename(main, orphan)
        redis.expire(orphan, settings().orphan_ttl)
    except Exception:
        with guard("orphan fallback delete"):
            redis.delete(main)


def read_logs(run_key) -> str:
    """The live terminal of a run, oldest line first."""
    if not run_key:
        return ""
    redis = client()
    if redis is None:
        return ""
    try:
        return "\n".join(redis.lrange(_log_key(run_key), 0, -1))
    except Exception:
        return ""


def read_orphan_logs(run_key) -> str:
    """What the previous attempt on this task id left behind."""
    if not run_key:
        return ""
    redis = client()
    if redis is None:
        return ""
    try:
        return "\n".join(redis.lrange(_orphan_key(run_key), 0, -1))
    except Exception:
        return ""


def read_any_logs(run_key) -> str:
    """The current buffer, or the orphan residue when there is no current one.

    Both reads go out in one pipeline -- one round trip, not two. The caller is
    typically an endpoint serving a *finished* run, which is exactly the path
    where Redis may be unreachable and each trip would cost a full timeout.
    """
    if not run_key:
        return ""
    redis = client()
    if redis is None:
        return ""
    try:
        pipe = redis.pipeline()
        pipe.lrange(_log_key(run_key), 0, -1)
        pipe.lrange(_orphan_key(run_key), 0, -1)
        current, orphan = pipe.execute()
        return "\n".join(current or orphan or [])
    except Exception:
        return ""


def clear_logs(run_key) -> None:
    """Drop a run's buffers once you have archived them.

    Takes the orphan residue with it: if it is still around when the current
    attempt closes cleanly, nobody is going to claim it. Both keys go in the same
    DELETE -- still one round trip.
    """
    if not run_key:
        return
    redis = client()
    if redis is None:
        return
    with guard("clear_logs"):
        redis.delete(_log_key(run_key), _orphan_key(run_key))


def clear_orphan_logs(run_key) -> None:
    """Drop only the previous attempt's residue (after archiving it)."""
    if not run_key:
        return
    redis = client()
    if redis is None:
        return
    with guard("clear_orphan_logs"):
        redis.delete(_orphan_key(run_key))


def cap_log(text: str, max_chars: Optional[int] = None) -> str:
    """Trim a terminal to a size your database column can hold, keeping the tail.

    The tail is the part you want: the traceback is at the end, the banner is not.
    """
    if not text:
        return text
    limit = max_chars if max_chars is not None else settings().max_archive_chars
    if len(text) <= limit:
        return text
    return "...[truncated]...\n" + text[-limit:]


DEFAULT_FORMAT = "[%(asctime)s] %(levelname)s %(name)s - %(message)s"


def install_logging(
    logger: Optional[logging.Logger] = None,
    level: int = logging.INFO,
    fmt: str = DEFAULT_FORMAT,
) -> LiveLogHandler:
    """Attach a :class:`LiveLogHandler` to ``logger`` (root logger by default).

    Idempotent: calling it twice on the same logger will not double every line.
    """
    target = logger or logging.getLogger()
    for existing in target.handlers:
        if isinstance(existing, LiveLogHandler):
            return existing
    handler = LiveLogHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt))
    target.addHandler(handler)
    return handler


def active_keys() -> Tuple[Optional[str], tuple]:
    """``(root key, extra keys)`` in this context. Mostly useful in tests."""
    return _current.get(), _extra.get()
