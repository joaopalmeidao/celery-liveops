import datetime

import pytest

import celery_liveops as liveops
from celery_liveops import watchdog


@pytest.fixture(autouse=True)
def clean_registry():
    yield
    watchdog._deadlines.clear()
    watchdog._enabled = None
    watchdog._timer = None


def test_deadline_belongs_to_the_task_not_the_container():
    """The whole point: one pool serves a 15-minute job and an 8-hour one."""
    liveops.set_deadlines({"quick.petition": 900, "overnight.sweep": 28800})

    assert liveops.deadline_for("quick.petition") == 900
    assert liveops.deadline_for("overnight.sweep") == 28800


def test_unregistered_task_falls_back_to_the_default():
    liveops.configure(default_deadline=1234)
    assert liveops.deadline_for("nobody.knows") == 1234
    assert liveops.deadline_for() == 1234


def test_safe_stop_leaves_room_for_teardown():
    liveops.set_deadline("crawl", 1000)
    start = datetime.datetime(2026, 1, 1, 12, 0, 0)

    stop_by = liveops.safe_stop_at(start, "crawl")

    assert stop_by == start + datetime.timedelta(seconds=900)   # 90% of the budget


def test_safe_stop_never_goes_below_a_minute():
    """Even a tiny deadline must leave enough time to write a checkpoint."""
    liveops.set_deadline("tiny", 10)
    start = datetime.datetime(2026, 1, 1, 12, 0, 0)

    assert liveops.safe_stop_at(start, "tiny") == start + datetime.timedelta(seconds=60)


def test_disabled_by_default(monkeypatch):
    """A library that kills processes is opted into, never switched on by install."""
    liveops.configure()
    assert liveops.settings().watchdog_enabled is False

    killed = []
    monkeypatch.setattr(watchdog, "_terminate", lambda *a: killed.append(a))

    class Task:
        name = "demo"

    watchdog._arm(task_id="t1", task=Task())
    assert watchdog._timer is None


def test_arming_and_disarming_a_timer(monkeypatch):
    liveops.configure(watchdog_enabled=True)
    liveops.set_deadline("demo", 5)

    class Task:
        name = "demo"

    watchdog._arm(task_id="t1", task=Task())
    timer = watchdog._timer
    assert timer is not None and timer.is_alive()
    assert timer.interval == 5

    watchdog._disarm()
    assert watchdog._timer is None
    timer.join(timeout=2)
    assert timer.finished.is_set(), "a disarmed timer must never fire"


def test_a_second_task_replaces_the_first_timer():
    liveops.configure(watchdog_enabled=True)

    class Task:
        name = "demo"

    watchdog._arm(task_id="t1", task=Task())
    first = watchdog._timer
    watchdog._arm(task_id="t2", task=Task())

    assert watchdog._timer is not first
    first.join(timeout=2)
    assert first.finished.is_set(), "the previous timer must be cancelled, not left armed"
    watchdog._disarm()


def test_timeout_hook_runs_before_the_process_dies(monkeypatch):
    seen = []
    exits = []
    monkeypatch.setattr(watchdog.os, "_exit", lambda code: exits.append(code))

    watchdog._on_timeout = lambda name, tid, timeout: seen.append((name, tid, timeout))
    watchdog._terminate("demo", "t9", 42)

    assert seen == [("demo", "t9", 42)]
    assert exits == [1]


def test_a_broken_hook_still_lets_the_process_die(monkeypatch):
    exits = []
    monkeypatch.setattr(watchdog.os, "_exit", lambda code: exits.append(code))

    watchdog._on_timeout = lambda *a: 1 / 0
    watchdog._terminate("demo", "t9", 42)

    assert exits == [1], "the whole point is freeing the queue; a hook cannot veto that"
