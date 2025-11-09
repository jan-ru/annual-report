#!/usr/bin/env python3
"""
Quick Module Validator
Validates that all modules can be imported and executed without the full test framework

Version: 0.2.0
"""

import sys
import tempfile
import shutil
from pathlib import Path

try:
    from version import __version__
except ImportError:
    __version__ = "0.2.0"

def quick_validate_module(module_name: str, modules_dir: Path, data_dir: Path) -> bool:
    """Quickly validate a single module"""
    
    module_file = modules_dir / f"{module_name}.py"
    if not module_file.exists():
        print(f"❌ Module file not found: {module_file}")
        return False
    
    # Create a temporary test environment
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Copy required files
        temp_modules = temp_path / "report_modules"
        temp_data = temp_path / "processed_data"
        
        shutil.copytree(modules_dir, temp_modules)
        shutil.copytree(data_dir, temp_data)
        
        # Create test script
        test_script = f'''
import sys
import os
sys.path.insert(0, os.getcwd())

try:
    # Run setup first
    exec(open('report_modules/setup.py').read())
    
    # Then run the module
    exec(open('report_modules/{module_name}.py').read())
    
    print("✅ Module validation passed")
    
except Exception as e:
    print(f"❌ Module validation failed: {{e}}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
'''
        
        test_file = temp_path / "validate.py"
        with open(test_file, 'w') as f:
            f.write(test_script)
        
        # Run the validation
        import subprocess
        import os
        
        original_cwd = os.getcwd()
        os.chdir(temp_path)
        
        try:
            result = subprocess.run([
                sys.executable, "validate.py"
            ], capture_output=True, text=True, timeout=30)
            
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

def main():
    """Quick validation of all modules"""
    
    print(f"🔍 Quick Module Validator v{__version__}")
    print()
    
    # Look for existing modular report structure
    possible_paths = [
        Path("reports/modular"),
        Path("modular_test_output"),
        Path("test/module_tests")
    ]
    
    modules_dir = None
    data_dir = None
    
    for path in possible_paths:
        if (path / "report_modules").exists() and (path / "processed_data").exists():
            modules_dir = path / "report_modules"
            data_dir = path / "processed_data"
            break
    
    if not modules_dir:
        print("❌ No modular report structure found!")
        print("Run this first: python telco_modular_implementation.py --sample-data")
        return 1
    
    print(f"🔍 Found modules in: {modules_dir}")
    print(f"🔍 Found data in: {data_dir}")
    print()
    
    # Available modules
    modules = [
        'setup',
        'executive_summary', 
        'income_statement',
        'balance_sheet',
        'financial_ratios',
        'revenue_charts',
        'asset_charts',
        'notes'
    ]
    
    print("🧪 Quick validation of all modules:")
    print("=" * 40)
    
    results = {}
    for module in modules:
        results[module] = quick_validate_module(module, modules_dir, data_dir)
    
    print()
    print("📊 VALIDATION SUMMARY:")
    print("=" * 40)
    
    passed = sum(results.values())
    total = len(results)
    
    for module, success in results.items():
        status = "PASS" if success else "FAIL"
        print(f"{module:20} {status}")
    
    print(f"\nOverall: {passed}/{total} modules passed")
    
    if passed == total:
        print("🎉 All modules validated successfully!")
        print("You can now use the full testing framework:")
        print("  python telco_test_module.py --all --render")
        return 0
    else:
        print("🚨 Some modules failed validation")
        print("Check the errors above before proceeding")
        return 1

if __name__ == "__main__":
    sys.exit(main())