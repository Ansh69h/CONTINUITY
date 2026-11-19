import json
import sys

from continuity import ContinuityRuntime, SideEffectClass, SQLiteCausalStore
import continuity.cli as cli
from continuity.cli import main


def test_cli_lists_inspects_and_verifies_runs_without_exposing_payload_by_default(tmp_path, capsys):
    database = str(tmp_path / "cli.db")
    store = SQLiteCausalStore(database)
    runtime = ContinuityRuntime(store=store, run_id="cli-run")
    runtime.step(lambda value: {"result": value}, "top-secret-input", step_id="cli-step")

    assert main(["runs", "list", "--database", database]) == 0
    assert "cli-run" in capsys.readouterr().out

    assert main(["runs", "inspect", "--database", database, "--run-id", "cli-run"]) == 0
    output = capsys.readouterr().out
    assert "STEP_COMMITTED" in output
    assert "top-secret-input" not in output
    assert "result" not in output

    assert main(["runs", "inspect", "--database", database, "--run-id", "cli-run", "--include-payload"]) == 0
    assert "top-secret-input" in capsys.readouterr().out

    assert main(["runs", "verify", "--database", database, "--run-id", "cli-run"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["passed"] is True


def test_cli_recovery_listing_reports_planner_decisions(tmp_path, capsys):
    database = str(tmp_path / "recovery.db")
    store = SQLiteCausalStore(database)
    runtime = ContinuityRuntime(store=store, run_id="recoverable-run")

    def uncertain_charge():
        raise OSError("network response lost")

    try:
        runtime.step(uncertain_charge, step_id="charge", side_effect=SideEffectClass.IRREVERSIBLE)
    except OSError:
        pass

    assert main(["runs", "recoveries", "--database", database, "--run-id", "recoverable-run"]) == 0
    output = capsys.readouterr().out
    assert "HALT_ESCALATE" in output
    assert "charge" in output


def test_cli_resume_requires_and_uses_the_selected_application_handler(tmp_path, capsys):
    database = str(tmp_path / "resume.db")
    module = tmp_path / "cli_handler.py"
    module.write_text(
        "def resume(value):\n    return {'resumed': value}\n",
        encoding="utf-8",
    )
    sys.path.insert(0, str(tmp_path))
    try:
        code = main(
            [
                "runs", "resume", "--database", database, "--run-id", "resume-run",
                "--step-id", "resume-step", "--handler", "cli_handler:resume",
                "--args-json", "[12]", "--side-effect", "PURE",
            ]
        )
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("cli_handler", None)

    assert code == 0
    result = json.loads(capsys.readouterr().out)
    assert result["result"] == {"resumed": 12}


def test_cli_resume_rejects_async_handler_before_recording_attempt(tmp_path, monkeypatch, capsys):
    database = str(tmp_path / "async-resume.db")

    async def async_handler():
        return "must not execute"

    monkeypatch.setattr(cli, "_load_object", lambda _spec: async_handler)
    code = main(
        [
            "runs", "resume", "--database", database, "--run-id", "async-run",
            "--step-id", "async-step", "--handler", "fake:async_handler",
        ]
    )

    assert code == 1
    assert "async handler" in json.loads(capsys.readouterr().out)["message"]
    assert SQLiteCausalStore(database).get_events("async-run") == []
