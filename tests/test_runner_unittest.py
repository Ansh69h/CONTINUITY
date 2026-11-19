import importlib.util
import os
import sys
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


class ContinuityFullSuiteTest(unittest.TestCase):
    pass


def _load_function_tests():
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    for root, _, files in os.walk(tests_dir):
        for fn in sorted(files):
            if fn.startswith("test_") and fn.endswith(".py") and fn != "test_runner_unittest.py":
                path = os.path.join(root, fn)
                spec = importlib.util.spec_from_file_location(fn[:-3], path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                for attr in sorted(dir(mod)):
                    if attr.startswith("test_") and callable(getattr(mod, attr)):
                        func = getattr(mod, attr)
                        setattr(ContinuityFullSuiteTest, f"{fn[:-3]}__{attr}", lambda self, f=func: f())


_load_function_tests()

if __name__ == "__main__":
    unittest.main(verbosity=2)
