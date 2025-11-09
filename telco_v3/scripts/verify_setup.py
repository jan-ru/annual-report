#!/usr/bin/env python3
"""
Setup Verification Script
Checks if the environment is properly configured
"""

import sys
import subprocess
from pathlib import Path


def check_python_version():
    """Check Python version"""
    version = sys.version_info
    print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 10):
        print("  ⚠️  Warning: Python 3.10+ recommended")
        return False
    return True


def check_uv_installed():
    """Check if uv is installed"""
    try:
        result = subprocess.run(
            ["uv", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        version = result.stdout.strip()
        print(f"✓ uv installed: {version}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("✗ uv not found")
        print("  Install: curl -LsSf https://astral.sh/uv/install.sh | sh")
        return False


def check_virtual_env():
    """Check if running in virtual environment"""
    in_venv = sys.prefix != sys.base_prefix
    if in_venv:
        print(f"✓ Virtual environment active: {sys.prefix}")
    else:
        print("✗ Not in virtual environment")
        print("  Run: uv venv && source .venv/bin/activate")
    return in_venv


def check_dependencies():
    """Check if key dependencies are installed"""
    required = ["polars", "pandas", "great_tables", "plotly", "jupyter"]
    missing = []
    
    for package in required:
        try:
            __import__(package)
            print(f"✓ {package} installed")
        except ImportError:
            print(f"✗ {package} missing")
            missing.append(package)
    
    if missing:
        print(f"\n  Install missing packages: uv pip install -e .")
        return False
    return True


def check_project_files():
    """Check if required project files exist"""
    required_files = [
        "pyproject.toml",
        "version.py",
        "main_polars_pivot.py",
        "modular_reporting_system.py",
        "telco_modular_implementation.py",
    ]
    
    all_exist = True
    for file in required_files:
        if Path(file).exists():
            print(f"✓ {file}")
        else:
            print(f"✗ {file} missing")
            all_exist = False
    
    return all_exist


def main():
    """Run all checks"""
    print("=" * 50)
    print("Financial Reporting System - Setup Verification")
    print("=" * 50)
    print()
    
    checks = [
        ("Python Version", check_python_version),
        ("uv Package Manager", check_uv_installed),
        ("Virtual Environment", check_virtual_env),
        ("Project Files", check_project_files),
        ("Dependencies", check_dependencies),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n{name}:")
        print("-" * 30)
        results.append(check_func())
    
    print("\n" + "=" * 50)
    if all(results):
        print("✓ All checks passed! Environment is ready.")
        print("\nNext steps:")
        print("  - Run tests: make test-all")
        print("  - Generate report: make run")
        return 0
    else:
        print("✗ Some checks failed. Please fix the issues above.")
        return 1


def cli():
    """CLI entry point"""
    sys.exit(main())


if __name__ == "__main__":
    cli()
