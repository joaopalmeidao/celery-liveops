import base64

import pytest

import celery_liveops as liveops
from celery_liveops import snapshots

PNG = b"\x89PNG\r\n\x1a\n-fake-bytes"


@pytest.fixture(autouse=True)
def reset_throttle():
    snapshots._last_capture = 0.0
    snapshots._in_flight.clear()
    snapshots.set_gate(None)
    snapshots._stats.update({"stored": 0, "failed": 0, "last_error": None})
    yield


def _flush():
    """Wait for the background store to finish (it is fire-and-forget)."""
    snapshots._executor.submit(lambda: None).result(timeout=5)


class FakeDriver:
    def __init__(self, png=PNG):
        self._png = png
        self.calls = 0

    def get_screenshot_as_png(self):
        self.calls += 1
        return self._png


def test_snapshot_stores_the_latest_frame(redis):
    driver = FakeDriver()
    with liveops.capture_logs("run-1"):
        assert liveops.snapshot(driver) is True
    _flush()

    stored = liveops.read_snapshot("run-1")
    assert base64.b64decode(stored) == PNG


def test_no_run_no_capture(redis):
    driver = FakeDriver()

    assert liveops.snapshot(driver) is False
    assert driver.calls == 0, "must not even touch the browser outside a run"


def test_throttled_so_it_can_live_inside_a_hot_loop(redis):
    driver = FakeDriver()
    liveops.configure(snapshot_min_interval=60)

    with liveops.capture_logs("run-2"):
        assert liveops.snapshot(driver) is True
        assert liveops.snapshot(driver) is False
        assert liveops.snapshot(driver) is False

    assert driver.calls == 1


def test_a_gate_can_turn_it_off_without_a_redeploy(redis):
    snapshots.set_gate(lambda: False)
    driver = FakeDriver()

    with liveops.capture_logs("run-3"):
        assert liveops.snapshot(driver) is False
    assert driver.calls == 0


def test_a_broken_gate_fails_open(redis):
    """A flag that errors must not blind the panel."""
    snapshots.set_gate(lambda: 1 / 0)
    driver = FakeDriver()

    with liveops.capture_logs("run-4"):
        assert liveops.snapshot(driver) is True


def test_a_failing_capture_is_counted_not_swallowed_silently(redis):
    class Broken:
        def get_screenshot_as_png(self):
            raise RuntimeError("browser is gone")

    with liveops.capture_logs("run-5"):
        assert liveops.snapshot(Broken()) is False

    stats = liveops.snapshot_stats()
    assert stats["failed"] == 1
    assert "browser is gone" in stats["last_error"]


def test_accepts_raw_bytes_and_callables(redis):
    with liveops.capture_logs("run-6"):
        assert liveops.snapshot(PNG) is True
    _flush()
    assert base64.b64decode(liveops.read_snapshot("run-6")) == PNG

    snapshots._last_capture = 0.0
    with liveops.capture_logs("run-7"):
        assert liveops.snapshot(lambda: PNG) is True
    _flush()
    assert base64.b64decode(liveops.read_snapshot("run-7")) == PNG


def test_accepts_a_wrapper_holding_a_driver(redis):
    class Wrapper:
        def __init__(self):
            self.driver = FakeDriver()

    with liveops.capture_logs("run-8"):
        assert liveops.snapshot(Wrapper()) is True


def test_a_property_that_raises_does_not_break_resolution(redis):
    """Since Selenium 4.30 a WebDriver exposes a `browser` property that raises
    when BiDi is unavailable -- and getattr with a default only swallows
    AttributeError."""

    class Hostile:
        def get_screenshot_as_png(self):
            return PNG

        @property
        def browser(self):
            raise RuntimeError("BiDi not available")

    with liveops.capture_logs("run-9"):
        assert liveops.snapshot(Hostile()) is True


def test_clear_snapshot(redis):
    with liveops.capture_logs("run-10"):
        liveops.snapshot(PNG)
    _flush()

    liveops.clear_snapshot("run-10")
    assert liveops.read_snapshot("run-10") is None


# ── FastAPI router ───────────────────────────────────────────────────────────


def test_router_refuses_to_be_mounted_without_auth():
    from celery_liveops.contrib.fastapi import liveops_router

    with pytest.raises(RuntimeError, match="dependencies"):
        liveops_router()


def test_router_serves_logs_presence_and_locks(redis):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from celery_liveops.contrib.fastapi import liveops_router

    app = FastAPI()
    app.include_router(liveops_router(public=True))
    client = TestClient(app)

    liveops.mark_alive("run-1")
    redis.rpush(liveops.logs._log_key("run-1"), "line one")
    liveops.register_lock(pattern="lock:x", label="X")
    redis.set("lock:x", "held", ex=60)

    body = client.get("/liveops/runs/run-1/logs").json()
    assert body["logs"] == "line one"
    assert body["alive"] is True

    assert client.post("/liveops/runs/alive", json={"task_ids": ["run-1", "nope"]}).json() == {
        "alive": ["run-1"]
    }
    assert client.get("/liveops/locks").json()["locks"][0]["key"] == "lock:x"
    assert client.post("/liveops/locks/release", json={"keys": ["lock:x"]}).json()["released"] == [
        "lock:x"
    ]


def test_router_can_be_read_only(redis):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from celery_liveops.contrib.fastapi import liveops_router

    app = FastAPI()
    app.include_router(liveops_router(public=True, allow_release=False))
    client = TestClient(app)

    # The route is not registered at all, so it is a 404 -- not a 403 that
    # would tell a caller the endpoint exists behind a permission.
    assert client.post("/liveops/locks/release", json={"keys": ["lock:x"]}).status_code == 404


def test_missing_snapshot_is_a_404(redis):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from celery_liveops.contrib.fastapi import liveops_router

    app = FastAPI()
    app.include_router(liveops_router(public=True))

    assert TestClient(app).get("/liveops/runs/nope/snapshot").status_code == 404
