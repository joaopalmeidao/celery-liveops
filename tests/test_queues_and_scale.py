"""Queue introspection and worker resizing.

The asymmetry between `queue_depth` and `has_consumer` is the point of this file:
one fails to ``None`` so a reaper does not invent failures, the other fails to
``True`` so a slow broker does not refuse legitimate work.
"""
import pytest

from celery_liveops import queues, scale


class FakeInspect:
    def __init__(self, mapping, raises=False):
        self._mapping = mapping
        self._raises = raises

    def active_queues(self):
        if self._raises:
            raise ConnectionError("broker is down")
        return self._mapping


class FakeControl:
    def __init__(self, mapping, raises=False):
        self._mapping = mapping
        self._raises = raises

    def inspect(self, timeout=None):
        return FakeInspect(self._mapping, self._raises)


class FakeApp:
    def __init__(self, mapping=None, declared=(), raises=False):
        self.control = FakeControl(mapping or {}, raises)

        class Conf:
            task_queues = {q: {} for q in declared}

        self.conf = Conf()


@pytest.fixture(autouse=True)
def unbind():
    yield
    queues.bind_app(None)


def test_consumers_are_mapped_from_one_sweep():
    queues.bind_app(FakeApp({"node-a": [{"name": "fast"}], "node-b": [{"name": "fast"}]}))

    assert queues.consumers_by_queue() == {"fast": ["node-a", "node-b"]}


def test_orphan_queue_is_the_quietest_failure_there_is():
    """Declared, published to, acknowledged as queued -- and nobody consuming."""
    app = FakeApp({"node-a": [{"name": "fast"}]}, declared=("fast", "forgotten"))
    queues.bind_app(app)

    assert queues.orphan_queues() == ["forgotten"]


def test_no_answer_accuses_nothing():
    """Without a measurement you raise no alert, or it fires on every blip."""
    queues.bind_app(FakeApp({}, declared=("fast",), raises=True))

    assert queues.orphan_queues() == []
    assert queues.consumers_by_queue() == {}


def test_has_consumer_is_optimistic_on_failure():
    queues.bind_app(FakeApp({}, raises=True))

    assert queues.has_consumer("fast") is True, (
        "callers fail fast before enqueueing; refusing real work because the "
        "broker was slow is worse than the wait"
    )


def test_has_consumer_is_honest_when_it_can_measure():
    queues.bind_app(FakeApp({"node-a": [{"name": "other"}]}))

    assert queues.has_consumer("fast") is False


def test_queue_depth_unknown_is_not_zero():
    queues.bind_app(FakeApp({}, raises=True))

    assert queues.queue_depth("fast") is None, (
        "a reaper must tell 'the queue is empty' from 'I could not ask'"
    )


# ── scale ────────────────────────────────────────────────────────────────────


class FakePool:
    def __init__(self, num_processes):
        self.num_processes = num_processes
        self.grown = 0
        self.shrunk = 0

    def grow(self, n):
        self.grown += n
        self.num_processes += n

    def shrink(self, n):
        self.shrunk += n
        self.num_processes -= n


class FakeAutoscaler:
    def __init__(self):
        self.max = None

    def update(self, max=None):
        self.max = max
        return max, 1


class FakeWorker:
    def __init__(self, processes=None, autoscaler=None):
        self.pool = FakePool(processes) if processes is not None else None
        self.autoscaler = autoscaler


def test_growing_a_prefork_pool():
    worker = FakeWorker(processes=2)

    assert scale.apply_target(worker, 5) is True
    assert worker.pool.num_processes == 5


def test_shrinking_a_prefork_pool():
    worker = FakeWorker(processes=6)

    assert scale.apply_target(worker, 2) is True
    assert worker.pool.num_processes == 2


def test_solo_pool_is_refused_instead_of_silently_doing_nothing():
    """Celery accepts the broadcast and changes nothing. Saying 'done' would be
    lying to whoever pressed the button."""
    worker = FakeWorker(processes=None)

    assert scale.apply_target(worker, 4) is False


def test_autoscaler_target_means_the_ceiling():
    """Growing the pool directly under --autoscale is undone at the first idle
    moment; the number the operator picks is the maximum."""
    autoscaler = FakeAutoscaler()
    worker = FakeWorker(processes=2, autoscaler=autoscaler)

    assert scale.apply_target(worker, 8) is True
    assert autoscaler.max == 8
    assert worker.pool.grown == 0, "the pool itself must not be grown"


def test_target_is_clamped_to_the_ceiling(monkeypatch):
    """Each browser executor is a Chrome at about a gigabyte: a mistyped number
    is an out-of-memory kill requested in advance."""
    monkeypatch.setenv("LIVEOPS_MAX_CONCURRENCY", "4")
    worker = FakeWorker(processes=1)

    scale.apply_target(worker, 99)

    assert worker.pool.num_processes == 4


def test_queues_of_process_reads_the_environment(monkeypatch):
    monkeypatch.setenv("CELERY_QUEUES", " fast , slow ,, ")

    assert scale.queues_of_process() == ["fast", "slow"]


def test_resize_is_a_noop_when_already_at_target():
    worker = FakeWorker(processes=3)

    assert scale.resize_pool(worker, 3) is False
    assert worker.pool.grown == 0 and worker.pool.shrunk == 0
