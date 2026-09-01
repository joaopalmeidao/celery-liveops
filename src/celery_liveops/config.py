"""Runtime configuration for celery-liveops.

Every knob has a working default, so `import celery_liveops` alone is enough for a
local Redis on ``localhost:6379``. Anything can be overridden by environment
variable (twelve-factor deployments) or by an explicit :func:`configure` call
(tests, embedding the library in an app that already owns its Redis client).

The one rule this module enforces: **nothing here may raise at import time**. The
library rides inside logging handlers and Celery signals, both of which run in
places where an exception is either swallowed or fatal to a worker boot.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Optional


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


@dataclass(frozen=True)
class Settings:
    """Immutable snapshot of the library's configuration."""

    #: Redis connection used for every live channel (logs, presence, snapshots).
    redis_url: str = field(
        default_factory=lambda: os.environ.get("LIVEOPS_REDIS_URL", "redis://localhost:6379/0")
    )
    #: Prefix for every key this library writes. Change it to share one Redis
    #: between several applications without them stepping on each other.
    key_prefix: str = field(
        default_factory=lambda: os.environ.get("LIVEOPS_KEY_PREFIX", "liveops")
    )

    # ── Live log buffer ───────────────────────────────────────────────────────
    #: Hard cap on lines kept per run. Peak memory per run is roughly
    #: ``max_lines * max_line_length`` -- deliberately conservative, because this
    #: buffer lives in the same Redis your broker results may be using.
    max_lines: int = field(default_factory=lambda: _env_int("LIVEOPS_MAX_LINES", 1500))
    #: Cap per line, so one giant record cannot blow the whole budget.
    max_line_length: int = field(default_factory=lambda: _env_int("LIVEOPS_MAX_LINE_LENGTH", 2000))
    #: Backstop expiry for a live buffer whose task died without cleaning up.
    log_ttl: int = field(default_factory=lambda: _env_int("LIVEOPS_LOG_TTL", 3600))
    #: Expiry for the *previous attempt's* buffer (see `logs.capture_logs`).
    orphan_ttl: int = field(default_factory=lambda: _env_int("LIVEOPS_ORPHAN_TTL", 900))
    #: Cap applied by :func:`celery_liveops.cap_log` before you archive a run's
    #: terminal into your own database.
    max_archive_chars: int = field(
        default_factory=lambda: _env_int("LIVEOPS_MAX_ARCHIVE_CHARS", 200_000)
    )

    # ── Presence ──────────────────────────────────────────────────────────────
    #: How long a presence key survives without a refresh. Generous relative to
    #: `presence_refresh` on purpose: one network hiccup must not mark a healthy
    #: run as dead.
    presence_ttl: int = field(default_factory=lambda: _env_int("LIVEOPS_PRESENCE_TTL", 90))
    #: Refresh interval of the daemon thread that keeps presence warm.
    presence_refresh: int = field(default_factory=lambda: _env_int("LIVEOPS_PRESENCE_REFRESH", 20))
    #: TTL of the per-worker heartbeat and "what am I running" keys.
    worker_ttl: int = field(default_factory=lambda: _env_int("LIVEOPS_WORKER_TTL", 60))

    # ── Watchdog ──────────────────────────────────────────────────────────────
    #: Master switch. Off by default: a library that kills processes must be
    #: opted into explicitly, never enabled by the mere act of installing it.
    watchdog_enabled: bool = field(
        default_factory=lambda: _env_bool("LIVEOPS_WATCHDOG_ENABLED", False)
    )
    #: Deadline for tasks with no registered deadline of their own.
    default_deadline: int = field(
        default_factory=lambda: _env_int("LIVEOPS_DEFAULT_DEADLINE", 900)
    )
    #: Fraction of the deadline at which a cooperative task should stop itself.
    #: The remaining 10% pays for teardown: close the browser, write a
    #: checkpoint, mark the run finished.
    safe_fraction: float = field(default_factory=lambda: _env_float("LIVEOPS_SAFE_FRACTION", 0.9))

    # ── Snapshots ─────────────────────────────────────────────────────────────
    #: Only the *latest* frame is kept, so this is a short "is it still on the
    #: login page?" TTL, not a history.
    snapshot_ttl: int = field(default_factory=lambda: _env_int("LIVEOPS_SNAPSHOT_TTL", 300))
    #: Minimum seconds between two captures. Grabbing a screenshot is a
    #: synchronous round-trip to the browser; unthrottled it competes with the
    #: work you are trying to watch.
    snapshot_min_interval: float = field(
        default_factory=lambda: _env_float("LIVEOPS_SNAPSHOT_MIN_INTERVAL", 1.2)
    )
    snapshots_enabled: bool = field(
        default_factory=lambda: _env_bool("LIVEOPS_SNAPSHOTS_ENABLED", True)
    )

    # ── Broker introspection ──────────────────────────────────────────────────
    #: Timeout for `celery inspect`. Short on purpose: a worker that cannot
    #: answer in two seconds is busy or dead, and either way the number you want
    #: is what the *others* answered.
    inspect_timeout: float = field(
        default_factory=lambda: _env_float("LIVEOPS_INSPECT_TIMEOUT", 2.0)
    )


_settings = Settings()
_redis_client: Any = None
_redis_factory: Optional[Callable[[], Any]] = None


def configure(
    *,
    redis_client: Any = None,
    redis_factory: Optional[Callable[[], Any]] = None,
    **overrides: Any,
) -> Settings:
    """Override configuration at runtime.

    ``redis_client`` lets an application hand over the connection it already
    manages (pool sizing, TLS, sentinel) instead of having this library open a
    second one. ``redis_factory`` does the same lazily, which is what you want
    when the client must be created after a fork.

    Any remaining keyword is a :class:`Settings` field::

        configure(key_prefix="billing", max_lines=500, watchdog_enabled=True)
    """
    global _settings, _redis_client, _redis_factory

    if overrides:
        unknown = set(overrides) - {f for f in Settings.__dataclass_fields__}
        if unknown:
            raise TypeError(f"unknown setting(s): {', '.join(sorted(unknown))}")
        _settings = replace(_settings, **overrides)

    if redis_client is not None or redis_factory is not None:
        _redis_client = redis_client
        _redis_factory = redis_factory

    return _settings


def settings() -> Settings:
    """The configuration in force right now."""
    return _settings


def reset() -> None:
    """Restore defaults (re-reading the environment). Mainly for tests."""
    global _settings, _redis_client, _redis_factory
    _settings = Settings()
    _redis_client = None
    _redis_factory = None
