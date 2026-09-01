import logging

import pytest

import celery_liveops as liveops
from celery_liveops import logs


@pytest.fixture
def log(redis):
    """A logger with the live handler attached, isolated from the root logger."""
    logger = logging.getLogger("test.liveops")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logs.install_logging(logger)
    return logger


def test_lines_land_in_the_run_buffer(log, redis):
    with liveops.capture_logs("run-1"):
        log.info("first")
        log.info("second")

    captured = liveops.read_logs("run-1")
    assert "first" in captured
    assert "second" in captured
    assert captured.index("first") < captured.index("second"), "order must be chronological"


def test_no_run_no_writes(log, redis):
    log.info("nobody is watching")
    assert redis.keys("*") == []


def test_install_logging_is_idempotent(log):
    before = len(log.handlers)
    logs.install_logging(log)
    assert len(log.handlers) == before


def test_current_key_is_visible_deep_in_the_stack(log, redis):
    def deep():
        return liveops.current_key()

    assert liveops.current_key() is None
    with liveops.capture_logs("run-2"):
        assert deep() == "run-2"
    assert liveops.current_key() is None


def test_extra_key_copies_lines_without_moving_the_root(log, redis):
    with liveops.capture_logs("batch"):
        log.info("batch started")
        with liveops.capture_logs("item-7", extra=True):
            log.info("item 7")
            # The root stays the task id: cancellation and snapshots depend on it.
            assert liveops.current_key() == "batch"
        log.info("batch done")

    assert "item 7" in liveops.read_logs("item-7")
    assert "batch started" not in liveops.read_logs("item-7")
    batch = liveops.read_logs("batch")
    assert "batch started" in batch and "item 7" in batch and "batch done" in batch


def test_long_lines_are_truncated(log, redis):
    liveops.configure(max_line_length=50)
    with liveops.capture_logs("run-3"):
        log.info("x" * 500)
    line = liveops.read_logs("run-3")
    assert "[line truncated]" in line
    assert len(line) < 200


def test_buffer_is_capped(log, redis):
    liveops.configure(max_lines=10)
    with liveops.capture_logs("run-4"):
        for i in range(50):
            log.info("line %s", i)
    kept = liveops.read_logs("run-4").splitlines()
    assert len(kept) == 10
    assert "line 49" in kept[-1], "the cap must keep the newest lines"


def test_retry_archives_the_previous_attempt_instead_of_deleting_it(log, redis):
    # Celery keeps the task id across retries. The first attempt dies without
    # cleanup; the second must not destroy the evidence.
    with liveops.capture_logs("same-id"):
        log.info("crashed here")

    with liveops.capture_logs("same-id"):
        log.info("second attempt")

    assert "crashed here" in liveops.read_orphan_logs("same-id")
    assert "crashed here" not in liveops.read_logs("same-id")
    assert "second attempt" in liveops.read_logs("same-id")


def test_read_any_falls_back_to_the_orphan(log, redis):
    with liveops.capture_logs("gone"):
        log.info("only evidence")
    # Simulate the reaper's view: the live buffer was rotated away.
    redis.rename(logs._log_key("gone"), logs._orphan_key("gone"))

    assert liveops.read_logs("gone") == ""
    assert "only evidence" in liveops.read_any_logs("gone")


def test_clear_removes_both_buffers(log, redis):
    with liveops.capture_logs("run-5"):
        log.info("a")
    with liveops.capture_logs("run-5"):
        log.info("b")

    liveops.clear_logs("run-5")
    assert liveops.read_logs("run-5") == ""
    assert liveops.read_orphan_logs("run-5") == ""


def test_redis_outage_never_reaches_the_task(broken_redis):
    logger = logging.getLogger("test.liveops.broken")
    logger.handlers.clear()
    logger.propagate = False
    logs.install_logging(logger)

    with liveops.capture_logs("run-6"):
        logger.info("the task keeps running")   # must not raise

    assert liveops.read_logs("run-6") == ""
    assert liveops.read_any_logs("run-6") == ""


def test_falsy_key_is_a_noop(log, redis):
    with liveops.capture_logs(None):
        log.info("outside a task")
    assert redis.keys("*") == []


def test_cap_log_keeps_the_tail():
    text = "\n".join(str(i) for i in range(10_000))
    capped = liveops.cap_log(text, max_chars=100)

    assert capped.startswith("...[truncated]...")
    assert capped.endswith("9999")          # the traceback is at the end
    assert len(capped) <= 100 + len("...[truncated]...\n")


def test_cap_log_leaves_short_text_alone():
    assert liveops.cap_log("short") == "short"
    assert liveops.cap_log("") == ""


def test_key_prefix_isolates_applications(log, redis):
    liveops.configure(key_prefix="tenant-a")
    with liveops.capture_logs("run-7"):
        log.info("hello")

    assert any(k.startswith("tenant-a:") for k in redis.keys("*"))
