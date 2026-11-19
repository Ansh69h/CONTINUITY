# Contributing

Thanks for helping improve CONTINUITY. Please open an issue before a large change so its behavior and compatibility impact can be discussed.

## Development setup

Requires Python 3.10 or later.

```powershell
git clone https://github.com/Ansh69h/CONTINUITY.git
cd CONTINUITY
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -W error::ResourceWarning tests/test_runner_unittest.py
```

For optional backend work, install the matching extra (`.[postgres]` or `.[redis]`) and run its tests against a real service before claiming live compatibility. Mocks are useful for unit tests but do not replace service-backed tests.

## Change expectations

- Add a regression test for bug fixes and behavior changes.
- Keep core dependencies optional unless a change is explicitly approved as a core requirement.
- Preserve synchronous and asynchronous behavior where applicable.
- Treat storage formats and public protocols as compatibility surfaces. Include migrations and explain any custom-store migration.
- Avoid logging payloads, secrets, or high-cardinality identifiers by default.
- Document failure semantics and limitations. Do not claim exactly-once external effects, formal verification, benchmark, soak, or live-service evidence that was not measured.
- Keep changes focused and include a changelog entry when user-visible behavior changes.

## Pull requests

Describe the problem, design, compatibility impact, tests run, and untested cases. Include a reproducible command for failures. Do not merge a pull request without the repository owner's review.

## Conduct

Contributors are expected to follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Vulnerabilities should be reported through the private channel in [SECURITY.md](SECURITY.md), not a public issue.
