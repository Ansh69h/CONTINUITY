import os
import subprocess
import sys
import tempfile
import time
from continuity import (
    SQLiteCausalStore,
    ContinuityRuntime,
    SideEffectClass,
    InvariantChecker,
)
from continuity.runtime import EscalationRequiredError


def test_real_os_exit_kill_and_persistent_restart_recovery():
    """Spawn a real child process that mutates external state and calls os._exit(137) before commit."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "causal.db")
        ext_ledger = os.path.join(tmpdir, "external_charges.txt")
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        child_code = (
            "import os, sys\n"
            f"sys.path.insert(0, {repo_root!r})\n"
            "from continuity import SQLiteCausalStore, ContinuityRuntime, SideEffectClass\n"
            f"store = SQLiteCausalStore({db_path!r})\n"
            "rt = ContinuityRuntime(store=store, run_id='kill-test-run', worker_id='victim-worker', lease_ttl=0.05)\n"
            "def charge_customer(amount):\n"
            f"    with open({ext_ledger!r}, 'a') as f:\n"
            "        f.write(f'CHARGED:{amount}\\n')\n"
            "        f.flush()\n"
            "        os.fsync(f.fileno())\n"
            "    return {'charged': amount}\n"
            "def kill_now():\n"
            "    os._exit(137)\n"
            "rt.step(\n"
            "    charge_customer,\n"
            "    250,\n"
            "    step_id='charge_step',\n"
            "    side_effect=SideEffectClass.IRREVERSIBLE,\n"
            "    before_commit_hook=kill_now,\n"
            ")\n"
        )
        proc = subprocess.run([sys.executable, "-c", child_code], capture_output=True, text=True)
        assert proc.returncode == 137, f"Expected 137, got {proc.returncode}, stderr={proc.stderr}"
        time.sleep(0.06)

        with open(ext_ledger, "r") as f:
            lines = f.read().strip().splitlines()
        assert lines == ["CHARGED:250"]

        store = SQLiteCausalStore(db_path)
        rt_recovered = ContinuityRuntime(store=store, run_id="kill-test-run", worker_id="recovery-worker")

        try:
            rt_recovered.step(
                lambda amt: {"charged": amt},
                250,
                step_id="charge_step",
                side_effect=SideEffectClass.IRREVERSIBLE,
            )
            assert False, "Expected EscalationRequiredError on un-reconciled IRREVERSIBLE step"
        except EscalationRequiredError:
            pass

        def reconcile_charge(idem_key: str):
            if os.path.exists(ext_ledger):
                with open(ext_ledger, "r") as f:
                    if "CHARGED:250" in f.read():
                        return (True, {"charged": 250})
            return (False, None)

        result = rt_recovered.step(
            lambda amt: (_ for _ in ()).throw(AssertionError("Must not re-execute charge!")),
            250,
            step_id="charge_step",
            side_effect=SideEffectClass.IRREVERSIBLE,
            reconciler=reconcile_charge,
        )
        assert result == {"charged": 250}

        with open(ext_ledger, "r") as f:
            assert f.read().strip().splitlines() == ["CHARGED:250"]

        checker = InvariantChecker()
        report = checker.check_run("kill-test-run", store.get_events("kill-test-run"), {"charge_step": 1})
        assert report.passed, f"Invariant violations: {report.violations}"
