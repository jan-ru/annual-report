"""
Module Validator CLI

Quick validation tool for report modules.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    from version import __version__
except ImportError:
    __version__ = "0.3.0"

_MODULES = [
    "data_loader",
    "executive_summary",
    "income_statement",
    "balance_sheet",
    "financial_ratios",
    "revenue_charts",
    "asset_charts",
    "notes",
]


def quick_validate_module(module_name: str, modules_dir: Path, data_dir: Path) -> bool:
    """Quickly validate a single report module by executing it in a temp directory."""
    module_file = modules_dir / f"{module_name}.py"
    if not module_file.exists():
        print(f"❌ Module file not found: {module_file}")
        return False

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        shutil.copytree(modules_dir, temp_path / "report_modules")
        shutil.copytree(data_dir, temp_path / "processed_data")

        loader = "data_loader" if (modules_dir / "data_loader.py").exists() else "setup"
        test_script = f"""
import sys, os
sys.path.insert(0, os.getcwd())
try:
    exec(open('report_modules/{loader}.py').read())
    exec(open('report_modules/{module_name}.py').read())
    print("✅ Module validation passed")
except Exception as e:
    import traceback
    print(f"❌ Module validation failed: {{e}}")
    traceback.print_exc()
    sys.exit(1)
"""
        test_file = temp_path / "validate.py"
        test_file.write_text(test_script)

        original_cwd = os.getcwd()
        os.chdir(temp_path)
        try:
            result = subprocess.run(
                [sys.executable, "validate.py"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                print(f"✅ {module_name}: {result.stdout.strip()}")
                return True
            else:
                print(f"❌ {module_name}: {result.stderr.strip()}")
                return False
        except subprocess.TimeoutExpired:
            print(f"❌ {module_name}: Validation timed out")
            return False
        except Exception as e:
            print(f"❌ {module_name}: Error during validation: {e}")
            return False
        finally:
            os.chdir(original_cwd)


def main() -> int:
    print(f"🔍 Quick Module Validator v{__version__}")
    print()

    possible_paths = [
        Path("reports/modular"),
        Path("modular_test_output"),
        Path("test/module_tests"),
    ]

    modules_dir = data_dir = None
    for path in possible_paths:
        if (path / "report_modules").exists() and (path / "processed_data").exists():
            modules_dir = path / "report_modules"
            data_dir = path / "processed_data"
            break

    if not modules_dir:
        print("❌ No modular report structure found!")
        print("Run first: python -m financial_reporting.cli.telco_report --sample-data")
        return 1

    print(f"🔍 Found modules in: {modules_dir}")
    print(f"🔍 Found data in: {data_dir}")
    print()
    print("🧪 Quick validation of all modules:")
    print("=" * 40)

    results = {module: quick_validate_module(module, modules_dir, data_dir) for module in _MODULES}

    print()
    print("📊 VALIDATION SUMMARY:")
    print("=" * 40)

    passed = sum(results.values())
    total = len(results)

    for module, success in results.items():
        print(f"{module:20} {'PASS' if success else 'FAIL'}")

    print(f"\nOverall: {passed}/{total} modules passed")

    if passed == total:
        print("🎉 All modules validated successfully!")
        return 0
    else:
        print("🚨 Some modules failed validation")
        return 1


if __name__ == "__main__":
    sys.exit(main())
