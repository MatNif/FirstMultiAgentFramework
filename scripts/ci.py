#!/usr/bin/env python3
"""
Local CI script for running all checks before committing
Usage: python scripts/ci.py
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: str, description: str) -> bool:
    """Run a command and return True if successful"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {cmd}")
    print('='*60)

    result = subprocess.run(cmd, shell=True, capture_output=False)

    if result.returncode == 0:
        print(f"✅ {description} - PASSED")
        return True
    else:
        print(f"❌ {description} - FAILED")
        return False


def main():
    """Run all CI checks"""
    project_root = Path(__file__).parent.parent

    # Change to project root
    import os
    os.chdir(project_root)

    print("🚀 Running local CI checks...")
    print(f"Project root: {project_root.absolute()}")

    checks = [
        ("ruff check .", "Ruff linting"),
        ("ruff format --check .", "Ruff formatting"),
        ("mypy . --ignore-missing-imports", "MyPy type checking"),
        ("pytest --cov=. --cov-report=term-missing --cov-fail-under=75", "Tests with coverage"),
    ]

    results = []
    for cmd, description in checks:
        success = run_command(cmd, description)
        results.append((description, success))

    print(f"\n{'='*60}")
    print("CI RESULTS SUMMARY")
    print('='*60)

    all_passed = True
    for description, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{description:<30} {status}")
        if not success:
            all_passed = False

    print('='*60)

    if all_passed:
        print("🎉 All checks passed! Ready to commit.")
        sys.exit(0)
    else:
        print("💥 Some checks failed. Please fix before committing.")
        sys.exit(1)


if __name__ == "__main__":
    main()