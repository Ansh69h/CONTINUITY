import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

import pytest

from continuity import SQLiteCausalStore


def test_sqlite_store_can_be_initialized_by_concurrent_workers(tmp_path):
    db_path = tmp_path / "concurrent-init.db"
    workers = 16
    start_together = Barrier(workers)

    def initialize_store(_):
        start_together.wait()
        try:
            SQLiteCausalStore(str(db_path))
        except Exception as exc:  # Capture each startup failure for a clear assertion.
            return exc
        return None

    with ThreadPoolExecutor(max_workers=workers) as executor:
        failures = list(executor.map(initialize_store, range(workers)))

    assert [failure for failure in failures if failure is not None] == []


def test_sqlite_initialization_preserves_wal_error_when_no_transaction_started(tmp_path):
    db_path = tmp_path / "wal-error.db"

    with patch.object(SQLiteCausalStore, "_enable_wal", side_effect=sqlite3.OperationalError("database is locked")):
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            SQLiteCausalStore(str(db_path))
