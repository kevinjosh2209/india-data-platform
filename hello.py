# ============================================================
# India Data Platform
# File    : hello.py
# Purpose : System environment verification
# Author  : Kevin Josh
# ============================================================

import sys
import platform
from datetime import datetime

def check_environment():
    """
    Verifies all required tools are installed and working.
    In production, this kind of check runs before every pipeline starts.
    """

    print("=" * 55)
    print("   INDIA DATA PLATFORM — ENVIRONMENT CHECK")
    print("=" * 55)
    print(f"   Timestamp  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Machine    : {platform.node()}")
    print(f"   OS         : {platform.system()} {platform.release()}")
    print("=" * 55)

    checks = []

    # Check 1 — Python version
    python_version = sys.version_info
    if python_version.major == 3 and python_version.minor >= 8:
        checks.append(("Python version", f"{python_version.major}.{python_version.minor}.{python_version.micro}", "PASS"))
    else:
        checks.append(("Python version", f"{python_version.major}.{python_version.minor}", "FAIL"))

    # Check 2 — Required libraries
    required_libraries = ["csv", "json", "urllib.request", "datetime", "logging"]
    for lib in required_libraries:
        try:
            __import__(lib)
            checks.append((f"Library: {lib}", "found", "PASS"))
        except ImportError:
            checks.append((f"Library: {lib}", "missing", "FAIL"))

    # Check 3 — Project folders
    import os
    required_folders = ["dags", "spark_jobs", "lambdas", "sql", "tests", "infrastructure"]
    for folder in required_folders:
        if os.path.exists(folder):
            checks.append((f"Folder: {folder}", "exists", "PASS"))
        else:
            checks.append((f"Folder: {folder}", "missing", "FAIL"))

    # Print results
    print(f"\n{'CHECK':<30} {'DETAIL':<20} {'STATUS'}")
    print("-" * 55)
    for check_name, detail, status in checks:
        print(f"{check_name:<30} {detail:<20} [{status}]")

    # Summary
    total  = len(checks)
    passed = sum(1 for _, _, s in checks if s == "PASS")
    failed = total - passed

    print("-" * 55)
    print(f"\n  Total checks : {total}")
    print(f"  Passed       : {passed}")
    print(f"  Failed       : {failed}")

    if failed == 0:
        print("\n  ✅ ALL CHECKS PASSED — environment is ready")
        print("  Pipeline can proceed safely\n")
    else:
        print("\n  ❌ SOME CHECKS FAILED — fix issues before proceeding\n")

    print("=" * 55)


if __name__ == "__main__":
    check_environment()