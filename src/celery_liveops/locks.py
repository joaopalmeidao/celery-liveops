"""The locks a dead process leaves behind -- named, visible, releasable.

Plenty of code takes a Redis key to serialise work that is not reentrant: one
login per account, one scrape per catalogue, one certificate loaded per
container. It is released in a ``finally``. When the process never reaches that
``finally`` -- container restart, OOM, a watchdog calling ``os._exit``, a deploy
mid-run -- the key stays, and it keeps blocking new work until its TTL expires,
which for a long job can be hours.

The symptom is always the same and it is miserable to diagnose: nothing shows an
error. The trigger "works", the task exits immediately with "already running",
and the screen says nothing. Killing the row in your panel does not help either:
the bookkeeping is in your database, the lock is in Redis.

This module gives those keys a **name**, so an operator can see and release them
from your own UI instead of reaching for ``redis-cli`` in production. Two
guarantees:

- **Allowlist.** Only keys matching a registered pattern can be released. The
  endpoint receives a raw key from the browser; without this, an arbitrary
  ``DEL`` against production Redis is one POST away.
- **No guessing.** Releasing a lock held by a *live* process is worse than the
  problem it fixes. So the panel shows the remaining TTL, and automatic release
  is limited to locks that unambiguously belong to the run being killed (see
  :func:`locks_owned_by`).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Dict, Iterable, List, Optional, Tuple

from .store import client

logger = logging.getLogger(__name__)

#: Ceiling on keys scanned per pattern. Legitimate locks are few (one per
#: tenant, per account, per container); a large number means something else is
#: going on, and the screen should not try to draw thousands of rows.
MAX_PER_PATTERN = 200


@dataclass(frozen=True)
class LockSpec:
    """A family of Redis keys that serialises work.

    ``pattern`` is a glob in ``SCAN MATCH`` style -- which is also what Python's
    :mod:`fnmatch` understands, so the same string both scans and validates.

    ``owned_by`` lists the run types whose death may release this lock without
    ambiguity. Empty means "manual release only", which is the right default.
    """

    pattern: str
    label: str
    module: str = ""
    blocks: str = ""
    owned_by: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def is_singleton(self) -> bool:
        """A pattern with no wildcard is one global key."""
        return "*" not in self.pattern and "?" not in self.pattern


_registry: List[LockSpec] = []


def register_lock(
    pattern: str,
    label: str,
    module: str = "",
    blocks: str = "",
    owned_by: Iterable[str] = (),
) -> LockSpec:
    """Add a lock family to the catalogue.

    ``blocks`` is the sentence your operator reads at 2am. Write what *stops
    happening* while the key is held, not what the key is::

        register_lock(
            pattern="lock:import:*",
            label="Catalogue import (one per tenant)",
            blocks="New imports for that tenant exit immediately with "
                   "'already running', including the manual button.",
        )
    """
    spec = LockSpec(
        pattern=pattern,
        label=label,
        module=module,
        blocks=blocks,
        owned_by=tuple(owned_by or ()),
    )
    _registry[:] = [s for s in _registry if s.pattern != pattern] + [spec]
    return spec


def registered_locks() -> List[LockSpec]:
    """The catalogue as registered."""
    return list(_registry)


def clear_registry() -> None:
    """Forget every registered lock (tests)."""
    _registry.clear()


def lock_for(lock_key: str) -> Optional[LockSpec]:
    """The spec covering this key, or ``None`` if it is not on the allowlist."""
    lock_key = (lock_key or "").strip()
    if not lock_key:
        return None
    for spec in _registry:
        if fnmatch(lock_key, spec.pattern):
            return spec
    return None


def list_locks() -> List[dict]:
    """Every registered lock currently held, with its remaining TTL.

    ``ttl`` comes back as ``None`` when the key has no expiry -- the worst case,
    and the one your screen most needs to be able to say out loud, because a lock
    with no TTL only ever leaves by human action.
    """
    redis = client()
    if redis is None:
        return []

    found: List[dict] = []
    for spec in _registry:
        keys = []
        try:
            for i, lock_key in enumerate(redis.scan_iter(match=spec.pattern, count=100)):
                if i >= MAX_PER_PATTERN:
                    logger.warning(
                        "celery-liveops: more than %s keys match %s; listing truncated.",
                        MAX_PER_PATTERN,
                        spec.pattern,
                    )
                    break
                keys.append(lock_key)
        except Exception as exc:
            logger.warning("celery-liveops: scanning %s failed: %s", spec.pattern, exc)
            continue

        for lock_key in keys:
            try:
                ttl = redis.ttl(lock_key)
            except Exception:
                ttl = -1
            try:
                value = redis.get(lock_key)
            except Exception:
                # Counters and strings share one catalogue; GET on a non-string
                # raises, and the value is informational anyway.
                value = None
            found.append(
                {
                    "key": lock_key,
                    "module": spec.module,
                    "label": spec.label,
                    "blocks": spec.blocks,
                    # Redis: -1 means no expiry, -2 means the key vanished
                    # between the scan and the ttl call.
                    "ttl": None if ttl is not None and ttl < 0 else ttl,
                    "never_expires": ttl == -1,
                    "value": value,
                }
            )

    found.sort(key=lambda row: (row["module"], row["key"]))
    return found


def release_locks(keys: Iterable[str]) -> Dict[str, list]:
    """Delete the given keys, refusing any that are not on the allowlist.

    Returns ``{"released": [...], "refused": [{"key", "reason"}]}``. A refusal is
    not a request error: in a bulk release, one unknown key must not take the
    others down with it.
    """
    result: Dict[str, list] = {"released": [], "refused": []}
    redis = client()

    for raw in keys or []:
        lock_key = (raw or "").strip()
        if not lock_for(lock_key):
            result["refused"].append(
                {"key": lock_key, "reason": "Key is not in the registered lock catalogue."}
            )
            continue
        if redis is None:
            result["refused"].append({"key": lock_key, "reason": "Redis unavailable."})
            continue
        try:
            deleted = redis.delete(lock_key)
        except Exception as exc:
            result["refused"].append({"key": lock_key, "reason": f"Redis failure: {exc}"})
            continue
        if deleted:
            logger.info("celery-liveops: lock released manually: %s", lock_key)
            result["released"].append(lock_key)
        else:
            result["refused"].append(
                {"key": lock_key, "reason": "Lock was already gone (expired or released)."}
            )
    return result


def lock_state(lock_key: str) -> Optional[dict]:
    """``{"key", "ttl"}`` if the lock is held right now, else ``None``.

    This is for the code that is about to *dispatch* work, so it can tell the
    user why their click is not going to run anything. Without it the request is
    accepted, the task exits with "already running", and the screen shows
    nothing -- the silence that makes a healthy queue look broken.
    """
    redis = client()
    if redis is None or not lock_for(lock_key):
        return None
    try:
        if not redis.exists(lock_key):
            return None
        ttl = redis.ttl(lock_key)
    except Exception as exc:
        logger.warning("celery-liveops: reading lock %s failed: %s", lock_key, exc)
        return None
    return {"key": lock_key, "ttl": None if ttl is None or ttl < 0 else ttl}


def locks_owned_by(run_type: Optional[str]) -> List[str]:
    """Held locks that killing a run of this type may release on its own.

    Only singleton patterns qualify: they identify one global round and leave no
    doubt about ownership. Per-tenant and per-container patterns are excluded on
    purpose -- killing run X does not prove that ``lock:login:acct-42`` was its
    lock, and releasing another *live* run's lock is worse than letting an
    orphan expire.
    """
    if not run_type:
        return []
    redis = client()
    if redis is None:
        return []

    keys = []
    for spec in _registry:
        if run_type not in spec.owned_by or not spec.is_singleton:
            continue
        try:
            if redis.exists(spec.pattern):
                keys.append(spec.pattern)
        except Exception as exc:
            logger.warning("celery-liveops: reading lock %s failed: %s", spec.pattern, exc)
    return keys
