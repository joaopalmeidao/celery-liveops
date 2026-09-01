"""A live picture of what the task is looking at.

For a browser automation, the log line "clicking submit" is far less useful than
the screen it was about to click. This keeps exactly one frame per run -- the
latest -- so an operator can open the panel and see that the task has been
sitting on a login page for four minutes.

Three properties make it safe to call from inside a hot loop:

- **Throttled.** At most one capture per ``snapshot_min_interval`` seconds.
  Grabbing a screenshot is a synchronous round trip to the browser; unthrottled
  it competes with the work you are trying to watch.
- **Off the hot path.** Only the capture itself is synchronous (it has to be --
  a WebDriver is not thread-safe). Encoding and storing happen on a background
  thread, fire-and-forget, and are coalesced: if a store is still in flight the
  new frame is dropped, because the next one covers it anyway.
- **Silent on failure, but counted.** It never raises. It does keep counters and
  the last error (:func:`snapshot_stats`), because a bare ``except: return``
  makes a broken feature indistinguishable from an idle one.
"""
from __future__ import annotations

import base64
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional

from .config import settings
from .logs import current_key
from .store import client, guard
from .store import key as _k

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="liveops-shot")
_in_flight = threading.Event()
#: Negative infinity, NOT 0.0: `time.monotonic()` is the machine's uptime on
#: Linux, so on a freshly booted host (a CI runner, a container that just came
#: up) zero is not "long ago" -- it is a few seconds ago, and the first capture
#: of the process gets swallowed by the throttle.
_last_capture = float("-inf")
_stats = {"stored": 0, "failed": 0, "last_error": None}

#: Optional gate, e.g. a cached read of a feature flag. Fail-open by contract:
#: if your callable raises, snapshots stay on.
_gate: Optional[Callable[[], bool]] = None


def _shot_key(run_key: str) -> str:
    return _k("shot", run_key)


def set_gate(gate: Optional[Callable[[], bool]]) -> None:
    """Install a runtime on/off switch for snapshots.

    Typically a cached lookup of a config row, so an operator can turn the
    feature off during an incident without a redeploy. Cache it yourself: this
    is called on every capture attempt.
    """
    global _gate
    _gate = gate


def _enabled() -> bool:
    if not settings().snapshots_enabled:
        return False
    if _gate is None:
        return True
    try:
        return bool(_gate())
    except Exception:
        return True  # fail-open: a broken flag must not blind the panel


def _resolve_png(source: Any) -> Optional[bytes]:
    """Get PNG bytes out of whatever was handed in.

    Accepts raw ``bytes``, a zero-argument callable, a Selenium WebDriver, or a
    wrapper object holding one.

    Do **not** reach for ``getattr(x, "driver", ...)`` style lookups here without
    checking the capability first: since Selenium 4.30 a WebDriver exposes a
    ``browser`` property (the BiDi module) that *raises* when BiDi is not
    available -- and ``getattr`` with a default only swallows ``AttributeError``.
    The reliable test is "does it know how to take a screenshot?".
    """
    if source is None:
        return None
    if isinstance(source, (bytes, bytearray)):
        return bytes(source)
    if hasattr(source, "get_screenshot_as_png"):
        return source.get_screenshot_as_png()
    if callable(source):
        result = source()
        return bytes(result) if result else None
    for attr in ("driver", "browser", "webdriver"):
        inner = getattr(source, attr, None)
        if inner is not None and hasattr(inner, "get_screenshot_as_png"):
            return inner.get_screenshot_as_png()
    return None


def _store(run_key: str, png: bytes) -> None:
    try:
        redis = client()
        if redis is None:
            return
        encoded = base64.b64encode(png).decode("ascii")
        redis.set(_shot_key(run_key), encoded, ex=settings().snapshot_ttl)
        _stats["stored"] += 1
    except Exception as exc:
        _stats["failed"] += 1
        _stats["last_error"] = f"store: {type(exc).__name__}: {exc}"
    finally:
        _in_flight.clear()


def snapshot(source: Any, run_key: Optional[str] = None) -> bool:
    """Capture the current frame for the active run. ``True`` if one was taken.

    A no-op outside a run, when throttled, or when the gate is closed -- so it is
    safe to sprinkle through a driver without guarding every call site::

        driver.click(submit)
        snapshot(driver)
    """
    global _last_capture

    run_key = run_key or current_key()
    if not run_key:
        return False

    now = time.monotonic()
    if now - _last_capture < settings().snapshot_min_interval:
        return False
    _last_capture = now

    if not _enabled():
        return False

    try:
        png = _resolve_png(source)
    except Exception as exc:
        _stats["failed"] += 1
        _stats["last_error"] = f"capture: {type(exc).__name__}: {exc}"
        return False
    if not png:
        return False

    # Coalescing: a store already in flight means this frame can be dropped.
    if _in_flight.is_set():
        return False
    _in_flight.set()
    try:
        _executor.submit(_store, str(run_key), png)
    except Exception:
        _in_flight.clear()
        return False
    return True


def read_snapshot(run_key) -> Optional[str]:
    """The latest frame as a base64 string, or ``None``."""
    if not run_key:
        return None
    redis = client()
    if redis is None:
        return None
    try:
        return redis.get(_shot_key(run_key))
    except Exception:
        return None


def clear_snapshot(run_key) -> None:
    """Drop a run's frame (call it when the run finishes)."""
    if not run_key:
        return
    redis = client()
    if redis is None:
        return
    with guard("clear_snapshot"):
        redis.delete(_shot_key(run_key))


def snapshot_stats() -> dict:
    """Counters for this process: ``stored``, ``failed``, ``last_error``."""
    return dict(_stats)
