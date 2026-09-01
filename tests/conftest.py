"""Shared fixtures.

Every test runs against ``fakeredis`` rather than a live server: the library's
whole contract is about what happens when Redis misbehaves, and a fake lets us
stage a failure instead of waiting for one.
"""
import fakeredis
import pytest

import celery_liveops as liveops
from celery_liveops import config, locks, store


@pytest.fixture(autouse=True)
def clean_state():
    """Reset module-level state between tests."""
    yield
    config.reset()
    store.reset_client()
    locks.clear_registry()


@pytest.fixture
def redis(clean_state):
    """A fake Redis wired into the library."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    liveops.configure(redis_client=fake)
    return fake


class BrokenRedis:
    """A client where every command raises -- the outage we design against."""

    def __getattr__(self, name):
        def _boom(*args, **kwargs):
            raise ConnectionError("redis is down")

        return _boom


@pytest.fixture
def broken_redis(clean_state):
    liveops.configure(redis_client=BrokenRedis())
    return None
