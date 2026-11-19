import json
import sqlite3

import pytest

from continuity import CausalEvent, EventType, SideEffectClass, SQLiteCausalStore


def legacy_event_dict():
    return {
        "run_id": "legacy-run",
        "seq": 1,
        "event_id": "evt-legacy",
        "step_id": "legacy-step",
        "step_name": "legacy_step",
        "event_type": EventType.STEP_INTENT.value,
        "side_effect": SideEffectClass.PURE.value,
        "idempotency_key": "legacy-key",
        "parent_event_ids": [],
        "payload": {"args": []},
        "timestamp": 1.0,
        "worker_id": "legacy-worker",
        "attempt": 1,
    }


def test_new_event_serialization_includes_current_schema_version():
    event = CausalEvent.from_dict(legacy_event_dict())

    assert event.to_dict().get("schema_version") == 1


def test_unknown_future_event_schema_is_rejected():
    data = legacy_event_dict()
    data["schema_version"] = 999

    with pytest.raises(ValueError, match="schema version"):
        CausalEvent.from_dict(data)


@pytest.mark.parametrize("schema_version", [True, "1", 1.9])
def test_explicit_non_integer_event_schema_is_rejected(schema_version):
    data = legacy_event_dict()
    data["schema_version"] = schema_version

    with pytest.raises(ValueError, match="must be an integer"):
        CausalEvent.from_dict(data)


def test_sqlite_store_migrates_v08_event_and_lease_tables(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE causal_events (
            run_id TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL UNIQUE,
            step_id TEXT NOT NULL, step_name TEXT NOT NULL, event_type TEXT NOT NULL,
            side_effect TEXT NOT NULL, idempotency_key TEXT NOT NULL,
            parent_event_ids TEXT NOT NULL, payload TEXT NOT NULL, timestamp REAL NOT NULL,
            worker_id TEXT NOT NULL, attempt INTEGER NOT NULL, PRIMARY KEY (run_id, seq)
        );
        CREATE TABLE step_leases (
            run_id TEXT NOT NULL, step_id TEXT NOT NULL, worker_id TEXT NOT NULL,
            expires_at REAL NOT NULL, PRIMARY KEY (run_id, step_id)
        );
        """
    )
    legacy = legacy_event_dict()
    conn.execute(
        """INSERT INTO causal_events (
            run_id, seq, event_id, step_id, step_name, event_type, side_effect,
            idempotency_key, parent_event_ids, payload, timestamp, worker_id, attempt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            legacy["run_id"], legacy["seq"], legacy["event_id"], legacy["step_id"],
            legacy["step_name"], legacy["event_type"], legacy["side_effect"],
            legacy["idempotency_key"], json.dumps(legacy["parent_event_ids"]),
            json.dumps(legacy["payload"]), legacy["timestamp"], legacy["worker_id"],
            legacy["attempt"],
        ),
    )
    conn.commit()
    conn.close()

    store = SQLiteCausalStore(str(db_path))
    migrated = store.get_events("legacy-run")
    conn = sqlite3.connect(db_path)
    event_columns = {row[1] for row in conn.execute("PRAGMA table_info(causal_events)")}
    lease_columns = {row[1] for row in conn.execute("PRAGMA table_info(step_leases)")}
    conn.close()

    assert len(migrated) == 1
    assert migrated[0].schema_version == 1
    assert "schema_version" in event_columns
    assert "fencing_token" in lease_columns
