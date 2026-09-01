import pytest

import celery_liveops as liveops


@pytest.fixture
def catalogue(redis):
    liveops.register_lock(
        pattern="lock:catalogue_import",
        label="Catalogue import",
        module="import",
        blocks="New imports exit immediately with 'already running'.",
        owned_by=("catalogue_import",),
    )
    liveops.register_lock(
        pattern="lock:login:*",
        label="Login (one session per account)",
        module="auth",
        blocks="Other runs on the same account queue behind the login.",
    )
    return redis


def test_only_registered_keys_are_recognised(catalogue):
    assert liveops.lock_for("lock:catalogue_import") is not None
    assert liveops.lock_for("lock:login:acct-42") is not None
    assert liveops.lock_for("some:other:key") is None
    assert liveops.lock_for("") is None


def test_listing_shows_what_is_held_and_for_how_long(catalogue):
    catalogue.set("lock:login:acct-42", "worker-1", ex=120)

    rows = liveops.list_locks()

    assert len(rows) == 1
    assert rows[0]["key"] == "lock:login:acct-42"
    assert rows[0]["label"] == "Login (one session per account)"
    assert 0 < rows[0]["ttl"] <= 120
    assert rows[0]["never_expires"] is False


def test_a_lock_without_ttl_is_flagged(catalogue):
    """The worst case, and the one the screen most needs to name: it only ever
    leaves by human action."""
    catalogue.set("lock:catalogue_import", "worker-1")

    row = liveops.list_locks()[0]
    assert row["ttl"] is None
    assert row["never_expires"] is True


def test_release_deletes_a_known_key(catalogue):
    catalogue.set("lock:catalogue_import", "worker-1")

    result = liveops.release_locks(["lock:catalogue_import"])

    assert result["released"] == ["lock:catalogue_import"]
    assert result["refused"] == []
    assert not catalogue.exists("lock:catalogue_import")


def test_an_unknown_key_is_refused_not_deleted(catalogue):
    """The raw key arrives from a browser. Without the allowlist an arbitrary
    DEL against production Redis is one POST away."""
    catalogue.set("sessions:admin", "very-important")

    result = liveops.release_locks(["sessions:admin"])

    assert result["released"] == []
    assert result["refused"][0]["key"] == "sessions:admin"
    assert catalogue.exists("sessions:admin"), "an unknown key must survive untouched"


def test_one_bad_key_does_not_take_the_batch_down(catalogue):
    catalogue.set("lock:catalogue_import", "x")
    catalogue.set("lock:login:acct-1", "x")

    result = liveops.release_locks(
        ["lock:catalogue_import", "sessions:admin", "lock:login:acct-1"]
    )

    assert sorted(result["released"]) == ["lock:catalogue_import", "lock:login:acct-1"]
    assert len(result["refused"]) == 1


def test_releasing_an_expired_lock_is_reported_not_celebrated(catalogue):
    result = liveops.release_locks(["lock:catalogue_import"])

    assert result["released"] == []
    assert "already gone" in result["refused"][0]["reason"]


def test_lock_state_explains_why_a_trigger_will_do_nothing(catalogue):
    assert liveops.lock_state("lock:catalogue_import") is None

    catalogue.set("lock:catalogue_import", "worker-1", ex=300)
    state = liveops.lock_state("lock:catalogue_import")

    assert state["key"] == "lock:catalogue_import"
    assert 0 < state["ttl"] <= 300


def test_killing_a_run_only_releases_unambiguous_locks(catalogue):
    """A wildcard lock is never auto-released: killing run X does not prove that
    lock:login:acct-42 was its lock, and freeing a live run's lock is worse."""
    catalogue.set("lock:catalogue_import", "x")
    catalogue.set("lock:login:acct-42", "x")

    assert liveops.locks_owned_by("catalogue_import") == ["lock:catalogue_import"]
    assert liveops.locks_owned_by("something_else") == []
    assert liveops.locks_owned_by(None) == []


def test_registering_the_same_pattern_twice_replaces_it(redis):
    liveops.register_lock(pattern="lock:x", label="first")
    liveops.register_lock(pattern="lock:x", label="second")

    assert len(liveops.registered_locks()) == 1
    assert liveops.lock_for("lock:x").label == "second"


def test_outage_degrades_to_empty_instead_of_raising(broken_redis):
    liveops.register_lock(pattern="lock:x", label="x")

    assert liveops.list_locks() == []
    assert liveops.lock_state("lock:x") is None
    assert liveops.locks_owned_by("x") == []

    result = liveops.release_locks(["lock:x"])
    assert result["released"] == []
    assert "redis is down" in result["refused"][0]["reason"]
