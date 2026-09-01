"""The demo has to actually work -- it is the first thing anyone runs.

Broker calls are stubbed, so this exercises the wiring (routes, Redis keys, the
mounted router) without needing RabbitMQ. The stack itself is verified by
``docker compose up`` in demo/README.md.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import celery_liveops as liveops  # noqa: E402


@pytest.fixture
def demo_client(redis, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    assert fastapi  # imported for the skip guard

    # No broker in the test environment: stub the calls that reach for one.
    # Patched on the package, because that is where the demo imports them from.
    monkeypatch.setattr(liveops, "queue_depth", lambda q: 7)
    monkeypatch.setattr(liveops, "consumers_by_queue", lambda timeout=None: {"demo": ["node-a"]})
    monkeypatch.setattr(liveops, "orphan_queues", lambda timeout=None: ["forgotten"])

    from demo.app.api import api

    return TestClient(api)


def test_the_page_is_served(demo_client):
    body = demo_client.get("/").text
    assert "celery-liveops" in body
    assert "no signal" in body, "the panel must be able to say a run has no signal"


def test_runs_are_annotated_with_presence(demo_client, redis):
    from demo.app.tasks import RUNS_KEY, remember

    remember("task-alive", "slow_job")
    remember("task-dead", "stuck_job")
    liveops.mark_alive("task-alive")

    rows = demo_client.get("/demo/runs").json()["runs"]
    by_id = {r["task_id"]: r for r in rows}

    assert by_id["task-alive"]["alive"] is True
    assert by_id["task-dead"]["alive"] is False
    assert redis.llen(RUNS_KEY) == 2


def test_a_retried_run_is_listed_once(demo_client, redis):
    """Celery keeps the task id across retries: the same run coming back after
    its worker died is one run, not two cards sharing a terminal."""
    from demo.app.tasks import remember

    remember("same-id", "slow_job")
    remember("same-id", "slow_job")

    rows = demo_client.get("/demo/runs").json()["runs"]
    assert [r["task_id"] for r in rows] == ["same-id"]


def test_the_run_list_is_capped(demo_client, redis):
    from demo.app.tasks import RUNS_KEY, remember

    for i in range(40):
        remember(f"task-{i}", "slow_job")

    assert redis.llen(RUNS_KEY) == 25


def test_overview_feeds_the_header(demo_client):
    body = demo_client.get("/demo/overview").json()

    assert body["queue_depth"] == 7
    assert body["orphan_queues"] == ["forgotten"]
    assert body["consumers"] == {"demo": ["node-a"]}
    assert "snapshot_stats" in body


def test_the_liveops_router_is_mounted(demo_client, redis):
    liveops.mark_alive("task-1")

    assert demo_client.get("/liveops/runs/task-1/logs").json()["alive"] is True
    assert demo_client.get("/liveops/locks").status_code == 200


def test_demo_frames_are_valid_pngs():
    from demo.app.tasks import fake_frame

    png = fake_frame(3)
    assert png.startswith(b"\x89PNG\r\n\x1a\n")
    assert png.endswith(b"IEND\xae\x42\x60\x82")
