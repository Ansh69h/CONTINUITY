# Benchmark and soak tooling

This repository includes configurable benchmark and soak programs. Their presence does not mean the large scenarios have been executed or that CONTINUITY has a certified capacity limit.

## Configured benchmark scenarios

`benchmarks/scenarios.json` defines a six-scenario matrix: 100, 500, and 1,000 worker threads, each with 10,000 or 100,000 workflow executions. These are configuration targets only. Each run writes a JSON result with the selected configuration and measured outcomes. Each scenario defaults to as many run-history groups as workers to limit history growth; lower `--contention-groups` values create more competing writers and longer histories.

Install the checkout first, then use a small local measurement:

```powershell
python -m pip install -e .
python benchmarks/run_benchmarks.py --workers 8 --runs 500 --backend sqlite --output benchmark-smoke.json
```

To select a configured large scenario:

```powershell
python benchmarks/run_benchmarks.py --scenario 100w-10k --backend sqlite --output benchmark-100w-10k.json
```

The report includes per-workflow p50/p95/p99/mean latency, throughput, failures, process CPU time, Python traced memory, SQLite file size, and recovery latency. Use a new database file for comparable SQLite runs. Benchmark values depend on the host, Python build, filesystem, and background load. The in-memory backend is useful for runtime overhead comparison; it does not measure durable storage.

## Soak profiles

`tests/soak/soak_runtime.py` supports configured 1, 6, and 24-hour profiles. It periodically records completed and failed workflows, throughput, CPU time, traced memory, and SQLite file size as JSON Lines.

```powershell
python tests/soak/soak_runtime.py --duration-hours 1 --workers 4 --backend sqlite --database .continuity-soak.db --output soak-1h.jsonl
```

A short smoke run can validate the harness, but it is explicitly labeled `custom-short-run` and does not count as a 1/6/24-hour soak:

```powershell
python tests/soak/soak_runtime.py --duration-seconds 10 --report-interval-seconds 2 --backend memory --output soak-smoke.jsonl
```

Run the long profiles on an isolated host with adequate disk space. Keep the JSONL report with the machine and software details when comparing runs. This project does not currently provide a universal service-level objective or claim a completed 1/6/24-hour soak result.

## Local trial snapshot (2026-11-19)

These measurements were captured on Windows with Python 3.14.3 and an 8-logical-CPU host. They are reproducibility examples, not performance targets:

| Trial | Configuration | Result |
| --- | --- | --- |
| SQLite benchmark | 8 workers, 1,000 workflows, 4 contention groups, captured before the concurrent WAL-initialization fix | 1,000 completed, 0 failed; 2.63 workflows/second; p50/p95/p99 latency 2.53/6.19/10.29 seconds; 12.40 MB peak Python-traced memory. |
| Recovery samples | 10 injected-failure recovery cases on SQLite | 10 completed, 0 failed; mean 89.95 ms recovery latency. |
| SQLite soak smoke | 8 workers, custom 10-second duration | 198 completed, 0 failed; 18.99 workflows/second; 397,579 bytes peak Python-traced memory; 180,224-byte SQLite database after checkpoint. |

The short soak exposed simultaneous SQLite WAL initialization failures. The store now sets WAL during initialization and retries transient lock errors. A 16-thread regression test passes after that change, and the 8-worker soak was rerun successfully. This does not establish long-duration stability or a production capacity limit.
