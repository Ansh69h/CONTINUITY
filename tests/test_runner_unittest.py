"""Backward-compatible script entry point for the pytest suite."""

from pathlib import Path


def main() -> int:
    import pytest

    tests_dir = Path(__file__).resolve().parent
    return int(pytest.main(["-q", str(tests_dir), "--ignore=tests/test_runner_unittest.py"]))


if __name__ == "__main__":
    raise SystemExit(main())
