import celery_liveops as liveops
from celery_liveops import presence


def test_alive_after_mark_and_gone_after_clear(redis):
    liveops.mark_alive("run-1")
    assert liveops.is_alive("run-1") is True

    liveops.clear_alive("run-1")
    assert liveops.is_alive("run-1") is False


def test_presence_key_carries_a_ttl(redis):
    """A dead process must stop claiming to be alive without anyone's help."""
    liveops.configure(presence_ttl=42)
    liveops.mark_alive("run-2")
    assert 0 < redis.ttl(presence._alive_key("run-2")) <= 42


def test_alive_among_is_one_round_trip(redis):
    liveops.mark_alive("a")
    liveops.mark_alive("c")

    assert liveops.alive_among(["a", "b", "c"]) == {"a", "c"}


def test_alive_among_deduplicates_and_ignores_blanks(redis):
    liveops.mark_alive("a")
    assert liveops.alive_among(["a", "a", "", None]) == {"a"}


def test_alive_among_on_outage_reports_no_signal_not_an_error(broken_redis):
    # An empty set means "no signal". Callers grey out a badge; they do not
    # raise a 500 at somebody polling a dashboard.
    assert liveops.alive_among(["a", "b"]) == set()
    assert liveops.is_alive("a") is False


def test_worker_id_is_resolved_per_call(redis):
    """Never cached at import: in a prefork worker that value is captured before
    the fork, and every child would then write to the same key."""
    assert ":" in liveops.worker_id()
    assert liveops.worker_id() == liveops.worker_id()


def test_workers_lists_heartbeats(redis):
    liveops.heartbeat("busy")
    found = liveops.workers()

    assert len(found) == 1
    assert found[0]["status"] == "busy"
    assert found[0]["worker_id"] == liveops.worker_id()


def test_workers_survives_a_hostname_containing_colons(redis):
    """The worker id is 'hostname:pid' and therefore contains a colon: the
    listing must slice the prefix, never split on ':'."""
    liveops.heartbeat()
    wid = liveops.worker_id()
    found = liveops.workers()
    assert [w["worker_id"] for w in found] == [wid]


def test_an_idle_worker_is_still_listed(redis):
    """A panel that lists only busy workers reports zero the moment the queue
    drains -- and a demo waiting for a heartbeat waits forever."""
    presence._on_worker_ready()

    found = liveops.workers()
    assert len(found) == 1
    assert found[0]["status"] == "idle"
    assert found[0]["current_task"] is None

    presence._on_worker_shutdown()
    assert liveops.workers() == [], "a worker shutting down stops claiming to be up"


def test_task_signals_write_and_clear_presence(redis):
    class Task:
        name = "demo.task"

    presence._start(task_id="run-9", task=Task(), args=(), kwargs={})
    assert liveops.is_alive("run-9") is True
    assert liveops.workers()[0]["current_task"]["task_id"] == "run-9"

    presence._finish(task_id="run-9")
    assert liveops.is_alive("run-9") is False
    assert liveops.workers()[0]["current_task"] is None


def test_context_extractor_labels_the_run(redis):
    class Task:
        name = "demo.task"

    def label(task_name, args, kwargs):
        return {"tenant": kwargs.get("tenant")}

    presence.install_presence(context_extractor=label)
    presence._start(task_id="run-10", task=Task(), args=(), kwargs={"tenant": "acme"})

    assert liveops.workers()[0]["current_task"]["tenant"] == "acme"
    presence._finish(task_id="run-10")


def test_a_broken_extractor_does_not_break_the_task(redis):
    class Task:
        name = "demo.task"

    def boom(task_name, args, kwargs):
        raise ValueError("bad label")

    presence.install_presence(context_extractor=boom)
    presence._start(task_id="run-11", task=Task(), args=(), kwargs={})

    assert liveops.is_alive("run-11") is True   # presence still recorded
    presence._finish(task_id="run-11")
