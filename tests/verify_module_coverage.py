"""Verify that every custom_components/ramses_cc module meets >=95.0% statement coverage.

Used in CI and local test workflows to enforce the Home Assistant Silver
Integration Quality Scale (IQS) coverage threshold.
"""

from __future__ import annotations

import sys
from pathlib import Path

import coverage

REPO_ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = str(REPO_ROOT / "custom_components" / "ramses_cc")
COVERAGE_FILE = REPO_ROOT / ".coverage"


def main() -> int:
    """Validate per-module coverage from .coverage file."""
    if not COVERAGE_FILE.exists():
        print(
            f"ERROR: Coverage file not found at {COVERAGE_FILE}.\n"
            "Run 'coverage run -m pytest' before running this verification script."
        )
        return 1

    cov = coverage.Coverage(data_file=str(COVERAGE_FILE))
    cov.load()

    failed_modules: list[tuple[str, float]] = []

    for file_path in cov.get_data().measured_files():
        if not file_path.startswith(BASE_DIR) or not file_path.endswith(".py"):
            continue
        analysis = cov.analysis2(file_path)
        total = len(analysis[1])
        missing = len(analysis[3])
        if total == 0:
            continue
        pct = ((total - missing) / total) * 100
        if pct < 95.0:
            failed_modules.append((file_path.split("/")[-1], pct))

    if failed_modules:
        print("FAILED: The following modules are below 95.0% test coverage:")
        for mod, pct in sorted(failed_modules, key=lambda x: x[1]):
            print(f"  - {mod}: {pct:.2f}%")
        return 1

    print(
        "SUCCESS: 100% of custom_components/ramses_cc modules satisfy >=95.0% test coverage!"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
