"""celery-liveops -- see inside a long-running Celery task while it runs.

A Celery task that takes eight hours is a black box. The result backend tells you
what happened *after* it happened; ``flower`` tells you a task is running, not
what it is doing. This library fills that gap with five pieces that share one
Redis connection and one rule -- **observability must never break the job it is
observing**:

- ``logs``      -- the task's log lines, readable live from another process
- ``presence``  -- whether a run still has a living process behind it
- ``watchdog``  -- a hard deadline per *task*, not per container
- ``locks``     -- the Redis locks a dead process left behind, named and releasable
- ``queues``    -- queue depth, and the queue nobody is consuming
- ``scale``     -- resize a running worker, and keep that size across restarts

Quick start::

    from celery_liveops import install, capture_logs

    install(watchdog={"crawl": 3600}, watchdog_enabled=True)

    @app.task(bind=True)
    def crawl(self, url):
        with capture_logs(self.request.id):
            log.info("fetching %s", url)   # visible live, from your API

And on the reading side::

    from celery_liveops import read_any_logs, is_alive

    read_any_logs(task_id)   # the terminal, live or archived
    is_alive(task_id)        # or is this row an orphan?
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from .config import Settings, configure, reset, settings
from .locks import (
    LockSpec,
    clear_registry,
    list_locks,
    lock_for,
    lock_state,
    locks_owned_by,
    register_lock,
    registered_locks,
    release_locks,
)
from .logs import (
    LiveLogHandler,
    active_keys,
    cap_log,
    capture_logs,
    clear_logs,
    clear_orphan_logs,
    current_key,
    install_logging,
    read_any_logs,
    read_logs,
    read_orphan_logs,
)
from .presence import (
    alive_among,
    clear_alive,
    heartbeat,
    install_presence,
    is_alive,
    mark_alive,
    worker_id,
    workers,
)
from .queues import (
    bind_app,
    consumers_by_queue,
    has_consumer,
    orphan_queues,
    queue_depth,
)
from .scale import (
    apply_target,
    install_boot_scale,
    max_concurrency,
    pool_size,
    queues_of_process,
    resize_pool,
    set_autoscale_ceiling,
)
from .snapshots import (
    clear_snapshot,
    read_snapshot,
    set_gate,
    snapshot,
    snapshot_stats,
)
from .store import client as redis_client
from .store import reset_client
from .watchdog import (
    deadline_for,
    deadlines,
    install_watchdog,
    safe_stop_at,
    set_deadline,
    set_deadlines,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # config
    "Settings",
    "configure",
    "settings",
    "reset",
    "redis_client",
    "reset_client",
    "install",
    # logs
    "LiveLogHandler",
    "capture_logs",
    "current_key",
    "install_logging",
    "read_logs",
    "read_orphan_logs",
    "read_any_logs",
    "clear_logs",
    "clear_orphan_logs",
    "cap_log",
    "active_keys",
    # presence
    "install_presence",
    "is_alive",
    "alive_among",
    "workers",
    "worker_id",
    "mark_alive",
    "clear_alive",
    "heartbeat",
    # watchdog
    "install_watchdog",
    "set_deadline",
    "set_deadlines",
    "deadlines",
    "deadline_for",
    "safe_stop_at",
    # locks
    "LockSpec",
    "register_lock",
    "registered_locks",
    "clear_registry",
    "lock_for",
    "list_locks",
    "release_locks",
    "lock_state",
    "locks_owned_by",
    # queues
    "bind_app",
    "queue_depth",
    "consumers_by_queue",
    "has_consumer",
    "orphan_queues",
    # scale
    "apply_target",
    "resize_pool",
    "set_autoscale_ceiling",
    "install_boot_scale",
    "pool_size",
    "queues_of_process",
    "max_concurrency",
    # snapshots
    "snapshot",
    "read_snapshot",
    "clear_snapshot",
    "snapshot_stats",
    "set_gate",
]


def install(
    app: Any = None,
    logger: Any = None,
    presence: bool = True,
    watchdog: Optional[Dict[str, int]] = None,
    watchdog_enabled: Optional[bool] = None,
    context_extractor: Any = None,
    **config_overrides: Any,
) -> None:
    """Wire everything up in one call, from inside your worker process.

    ``watchdog`` is the deadline registry (``{task_name: seconds}``). It is armed
    only when ``watchdog_enabled`` is true, because it kills processes.

    Safe to call in a web process too: presence and the watchdog attach to Celery
    signals that simply never fire there.
    """
    if config_overrides:
        configure(**config_overrides)
    if app is not None:
        bind_app(app)
    install_logging(logger)
    if presence:
        install_presence(context_extractor=context_extractor)
    if watchdog or watchdog_enabled is not None:
        install_watchdog(deadlines=watchdog, enabled=watchdog_enabled)
