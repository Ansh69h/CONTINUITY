import importlib.util
import sys
import threading
import time
from pathlib import Path

import pytest


def _load_soak_runtime():
    path = Path(__file__).parent / "soak" / "soak_runtime.py"
    spec = importlib.util.spec_from_file_location("continuity_soak_runtime", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_soak_stops_workers_when_report_flush_fails(tmp_path, monkeypatch):
    soak = _load_soak_runtime()

    class FakeRun:
        instance = None

        def __init__(self, *_args, **_kwargs):
            self.stop_event = threading.Event()
            self.stop_called = False
            self.started_at = time.perf_counter()
            self.failure_count = 0
            FakeRun.instance = self

        def stop(self):
            self.stop_called = True
            self.stop_event.set()

        def worker(self, _worker_index):
            return None

        def snapshot(self):
            return {"completed_workflows": 0, "failed_workflows": 0}

    class FakeFuture:
        def result(self):
            return None

    class FakeExecutor:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            assert FakeRun.instance.stop_called, "workers must be stopped before executor shutdown"
            return False

        def submit(self, *_args, **_kwargs):
            return FakeFuture()

    real_open = Path.open

    class FailingFlushFile:
        def __init__(self, path, *args, **kwargs):
            self.file = real_open(path, *args, **kwargs)
            self.flush_count = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.file.__exit__(*args)

        def write(self, *args):
            return self.file.write(*args)

        def flush(self):
            self.flush_count += 1
            if self.flush_count == 2:
                raise OSError("simulated report flush failure")
            return self.file.flush()

    monkeypatch.setattr(soak, "SoakRun", FakeRun)
    monkeypatch.setattr(soak.concurrent.futures, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(Path, "open", lambda path, *args, **kwargs: FailingFlushFile(path, *args, **kwargs))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "soak_runtime.py",
            "--duration-seconds",
            "1",
            "--workers",
            "1",
            "--report-interval-seconds",
            "0.001",
            "--output",
            str(tmp_path / "report.jsonl"),
        ],
    )

    with pytest.raises(OSError, match="simulated report flush failure"):
        soak.main()

    assert FakeRun.instance.stop_called


def test_short_soak_waits_only_until_its_deadline(tmp_path, monkeypatch):
    soak = _load_soak_runtime()

    class DeadlineWait:
        def wait(self, timeout):
            assert timeout <= 0.1
            return True

    class FakeRun:
        def __init__(self, *_args, **_kwargs):
            self.stop_event = DeadlineWait()
            self.started_at = time.perf_counter()
            self.failure_count = 0
            self.stopped = False

        def stop(self):
            self.stopped = True

        def worker(self, _worker_index):
            return None

        def snapshot(self):
            return {"completed_workflows": 0, "failed_workflows": 0}

    class FakeFuture:
        def result(self):
            return None

    class FakeExecutor:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, *_args, **_kwargs):
            return FakeFuture()

    monkeypatch.setattr(soak, "SoakRun", FakeRun)
    monkeypatch.setattr(soak.concurrent.futures, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "soak_runtime.py",
            "--duration-seconds",
            "0.02",
            "--workers",
            "1",
            "--report-interval-seconds",
            "60",
            "--output",
            str(tmp_path / "short-report.jsonl"),
        ],
    )

    assert soak.main() == 0


def test_soak_refuses_output_that_would_truncate_sqlite_database(tmp_path, monkeypatch):
    soak = _load_soak_runtime()
    database = tmp_path / "soak.db"
    original = b"sqlite database content"
    database.write_bytes(original)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "soak_runtime.py",
            "--database",
            str(database),
            "--duration-seconds",
            "0.01",
            "--workers",
            "1",
            "--output",
            str(database),
        ],
    )

    with pytest.raises(SystemExit):
        soak.main()

    assert database.read_bytes() == original
