from dataclasses import dataclass
import threading
from threading import RLock, Thread, current_thread

import pytest

import pytest

from continuity import (
    CausalEvent,
    EventType,
    FencingTokenError,
    InMemoryCausalStore,
    SQLiteCausalStore,
    SideEffectClass,
)


@dataclass
class FakeClock:
    now: float = 1000.0

    def __call__(self):
        return self.now


class SampledClock:
    def __init__(self, now=0.0):
        self.now = now
        self.sampled = threading.Event()

    def __call__(self):
        self.sampled.set()
        return self.now


class TrackingRLock:
    def __init__(self):
        self._lock = RLock()
        self.worker_attempted = threading.Event()

    def acquire(self, *args, **kwargs):
        if current_thread() is not threading.main_thread():
            self.worker_attempted.set()
        return self._lock.acquire(*args, **kwargs)

    def release(self):
        self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_args):
        self.release()


def make_event():
    return CausalEvent(
        run_id="fenced-run",
        seq=1,
        event_id="evt-fenced",
        step_id="fenced-step",
        step_name="fenced_step",
        event_type=EventType.STEP_COMMITTED,
        side_effect=SideEffectClass.PURE,
        idempotency_key="stable-key",
        payload={"output": 42},
        timestamp=1000.0,
        worker_id="worker-b",
    )


def test_stale_worker_cannot_append_after_lease_expiry_and_takeover():
    clock = FakeClock()
    store = InMemoryCausalStore(clock=clock)
    acquire = getattr(store, "acquire_fenced_step_lease", None)
    assert callable(acquire), "store must expose fenced lease acquisition"

    lease_a = acquire("fenced-run", "fenced-step", "worker-a", ttl_seconds=5)
    assert lease_a is not None
    clock.now += 6
    lease_b = acquire("fenced-run", "fenced-step", "worker-b", ttl_seconds=5)
    assert lease_b is not None
    assert lease_b.fencing_token > lease_a.fencing_token

    with pytest.raises(RuntimeError, match="fencing token|lease"):
        store.append_event(make_event(), lease=lease_a)

    store.append_event(make_event(), lease=lease_b)
    assert store.get_latest_seq("fenced-run") == 1


def test_renewal_preserves_token_and_release_does_not_reset_sequence():
    clock = FakeClock()
    store = InMemoryCausalStore(clock=clock)
    acquire = getattr(store, "acquire_fenced_step_lease", None)
    assert callable(acquire), "store must expose fenced lease acquisition"

    first = acquire("fenced-run", "fenced-step", "worker-a", ttl_seconds=5)
    assert first is not None
    renew = getattr(store, "renew_fenced_step_lease", None)
    assert callable(renew), "store must expose token-preserving renewal"
    renewed = renew(first, ttl_seconds=5)
    assert renewed is not None
    assert renewed.fencing_token == first.fencing_token

    release = getattr(store, "release_fenced_step_lease", None)
    assert callable(release), "store must release the exact fenced lease"
    release(renewed)
    second = acquire("fenced-run", "fenced-step", "worker-b", ttl_seconds=5)

    assert second is not None
    assert second.fencing_token > first.fencing_token


def test_sqlite_fencing_token_survives_release_and_rejects_old_owner(tmp_path):
    clock = FakeClock()
    store = SQLiteCausalStore(str(tmp_path / "fenced.db"), clock=clock)
    first = store.acquire_fenced_step_lease("fenced-run", "fenced-step", "worker-a", ttl_seconds=5)
    assert first is not None
    store.release_fenced_step_lease(first)
    second = store.acquire_fenced_step_lease("fenced-run", "fenced-step", "worker-b", ttl_seconds=5)

    assert second is not None
    assert second.fencing_token > first.fencing_token
    with pytest.raises(RuntimeError, match="fencing token|lease"):
        store.append_event(make_event(), lease=first)


@pytest.mark.parametrize("ttl_seconds", [float("inf"), float("-inf"), float("nan")])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_local_stores_reject_non_finite_lease_ttls(backend, ttl_seconds, tmp_path):
    store = (
        InMemoryCausalStore()
        if backend == "memory"
        else SQLiteCausalStore(str(tmp_path / f"non-finite-{ttl_seconds}.db"))
    )
    with pytest.raises(ValueError, match="finite and positive"):
        store.acquire_fenced_step_lease("run", "step", "worker", ttl_seconds=ttl_seconds)


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("operation", ["acquire", "renew"])
def test_lease_expiry_uses_clock_after_waiting_for_local_lock(backend, operation, tmp_path):
    clock = SampledClock(now=0.0)
    if backend == "memory":
        store = InMemoryCausalStore(clock=clock)
    else:
        store = SQLiteCausalStore(str(tmp_path / f"clock-{operation}.db"), clock=clock)
    first = store.acquire_fenced_step_lease("clock-run", "clock-step", "worker-a", ttl_seconds=1)
    assert first is not None

    tracked_lock = TrackingRLock()
    store._lock = tracked_lock
    clock.sampled.clear()
    tracked_lock.acquire()
    result = []

    def contend_for_lease():
        if operation == "acquire":
            result.append(store.acquire_fenced_step_lease("clock-run", "clock-step", "worker-a", ttl_seconds=1))
        else:
            result.append(store.renew_fenced_step_lease(first, ttl_seconds=1))

    thread = Thread(target=contend_for_lease)
    thread.start()
    try:
        assert tracked_lock.worker_attempted.wait(timeout=2)
        clock.now = 2.0
    finally:
        tracked_lock.release()
        thread.join(timeout=5)
    assert not thread.is_alive()

    if operation == "acquire":
        assert result[0] is not None
        assert result[0].fencing_token > first.fencing_token
    else:
        assert result == [None]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_released_lease_is_invalid_even_when_clock_is_before_epoch(backend, tmp_path):
    clock = FakeClock(now=-100.0)
    if backend == "memory":
        store = InMemoryCausalStore(clock=clock)
    else:
        store = SQLiteCausalStore(str(tmp_path / "negative-clock.db"), clock=clock)
    lease = store.acquire_fenced_step_lease("fenced-run", "fenced-step", "worker-a", ttl_seconds=5)
    assert lease is not None
    store.release_fenced_step_lease(lease)

    with pytest.raises(FencingTokenError):
        store.append_event(make_event(), lease=lease)
    assert store.renew_fenced_step_lease(lease, ttl_seconds=5) is None
