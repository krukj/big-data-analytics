"""
Shared pytest configuration for all test suites.

Puts the project on ``sys.path`` the same way the code is run in the repository
(README: ``PYTHONPATH=.``; Airflow DAG: ``sys.path.append(".../src/big_data_analytics")``):
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PACKAGE = REPO_ROOT / "src" / "big_data_analytics"

for path in (str(SRC_PACKAGE), str(REPO_ROOT)):
    if path in sys.path:
        sys.path.remove(path)
    sys.path.insert(0, path)
