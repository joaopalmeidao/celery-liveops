# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-09-01

First public release.

### Added

- **logs** - per-run live terminal over a capped Redis list, with the previous
  attempt's buffer archived rather than deleted on retry (`read_orphan_logs`).
- **presence** - per-run and per-worker liveness written from Celery signals,
  read with `EXISTS`; `alive_among` answers for a whole page in one round trip.
- **watchdog** - a hard deadline registered per task name, so one pool serves
  both short and overnight jobs; `safe_stop_at` for cooperative stopping.
- **locks** - a named catalogue of the Redis locks a dead process leaves behind,
  with an allowlist on release and automatic release limited to singleton locks.
- **queues** - `queue_depth`, `consumers_by_queue`, `has_consumer` and
  `orphan_queues`, each failing in the direction its caller needs.
- **scale** - runtime pool resize, autoscaler ceiling, and boot-time
  reapplication of a persisted target.
- **snapshots** - throttled, background-stored last frame of a browser task.
- `contrib.fastapi.liveops_router`, which refuses to mount without auth.
- A four-container demo stack under `demo/`.
