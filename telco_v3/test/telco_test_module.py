#!/usr/bin/env python3
"""
Telco B.V. Module Testing Script
Test individual report modules and sections independently

Usage:
    python telco_test_module.py --setup
    python telco_test_module.py --income_statement
    python telco_test_module.py --balance_sheet --render
    python telco_test_module.py --all --render
"""

import argparse
import sys
import subprocess
import tempfile
from pathlib import Path
import os
import shutil
from datetime import datetime

# Ensure the project root is on sys.path so the installed package is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

class TelcoModuleTester:
    """Test individual modules of the Telco reporting system"""
    
    def __init__(self, test_dir: Path = None):
        self.test_dir = test_dir or Path(__file__).parent / "module_tests"
        self.test_dir.mkdir(exist_ok=True)
        
        # Available modules to test
        self.available_modules = {
            'data_loader': 'Setup and data loading',
            'executive_summary': 'Executive summary and highlights',
            'income_statement': 'Income statement display and analysis',
            'balance_sheet': 'Balance sheet display and analysis', 
            'financial_ratios': 'Financial ratios and KPIs',
            'revenue_charts': 'Revenue analysis charts',
            'asset_charts': 'Asset composition charts',
            'notes': 'Notes and methodology'
        }
    
    def setup_test_environment(self):
        """Set up the test environment with sample data"""
        print("🔧 Setting up test environment...")
        
        # Import and create sample data
        try:
            from financial_reporting.cli.telco_report import TelcoModularReportGenerator
            from financial_reporting.reporting.modular_system import ModularReportOrchestrator

            generator = TelcoModularReportGenerator("Telco B.V.", "2024")
            sample_data = generator.create_telco_sample_data()

            orchestrator = ModularReportOrchestrator("Telco B.V.", "2024")
            qmd_file = orchestrator.generate_full_report(
                sample_data['income_statement'],
                sample_data['balance_sheet'], 
                self.test_dir
            )
            
            print(f"✅ Test environment created in: {self.test_dir}")
            print(f"📊 Sample data and modules ready for testing")
            return True
            
        except Exception as e:
            print(f"❌ Error setting up test environment: {e}")
            return False
    
    def test_module(self, module_name: str, render: bool = False) -> bool:
        """Test a specific module"""
        
        if module_name not in self.available_modules:
            print(f"❌ Unknown module: {module_name}")
            print(f"Available modules: {', '.join(self.available_modules.keys())}")
            return False
        
        print(f"🧪 Testing module: {module_name}")
        print(f"Description: {self.available_modules[module_name]}")
        
        # Check if test environment exists
        modules_dir = self.test_dir / "report_modules"
        processed_data_dir = self.test_dir / "processed_data"
        
        if not modules_dir.exists() or not processed_data_dir.exists():
            print("⚠️ Test environment not found, setting up...")
            if not self.setup_test_environment():
                return False
        
        # Create a test script for the specific module
        test_script = self._create_module_test_script(module_name)
        test_file = self.test_dir / f"test_{module_name}.py"
        
        with open(test_file, 'w') as f:
            f.write(test_script)
        
        # Run the test
        print(f"🔄 Executing module test...")
        
        # Change to test directory for execution
        original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        
        try:
            result = subprocess.run([
                sys.executable, test_file.name
            ], capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                print(f"✅ Module test passed!")
                print("Output:", result.stdout[:500] + "..." if len(result.stdout) > 500 else result.stdout)
                
                # If render requested, create a mini report
                if render:
                    return self._render_module_report(module_name)
                return True
            else:
                print(f"❌ Module test failed!")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            print(f"❌ Module test timed out")
            return False
        except Exception as e:
            print(f"❌ Error running test: {e}")
            return False
        finally:
            os.chdir(original_cwd)
    
    def _create_module_test_script(self, module_name: str) -> str:
        """Create a test script for a specific module"""
        
        return f'''#!/usr/bin/env python3
"""
Test script for {module_name} module
Generated automatically by telco_test_module.py
"""

import sys
import traceback
from pathlib import Path

def test_{module_name}_module():
    """Test the {module_name} module"""
    
    print(f"Testing {module_name} module...")
    
    try:
        # First run setup to ensure data is loaded
        print("Loading setup module...")
        exec(open('report_modules/data_loader.py').read())
        print("✅ Setup module loaded successfully")
        
        # Then run the specific module
        print(f"Loading {module_name} module...")
        exec(open('report_modules/{module_name}.py').read())  
        print(f"✅ {module_name} module executed successfully")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ Module file not found: {{e}}")
        return False
    except Exception as e:
        print(f"❌ Error in {module_name} module: {{e}}")
        print("Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_{module_name}_module()
    sys.exit(0 if success else 1)
'''
    
    def _render_module_report(self, module_name: str) -> bool:
        """Create a mini Quarto report for just this module"""
        
        print(f"📄 Creating mini-report for {module_name}...")
        
        # Create a minimal QMD file for just this module
        qmd_content = f'''---
title: "Telco B.V. - {module_name.replace('_', ' ').title()} Test"
subtitle: "Module Testing Report"
author: "Module Tester"
date: "{datetime.now().strftime('%B %d, %Y')}"
format:
  html:
    theme: cosmo
    toc: false
    code-fold: true
execute:
  echo: false
  warning: false
  message: false
  error: false
jupyter: python3
---

# {module_name.replace('_', ' ').title()} Module Test
        
```{{python}}
# Load setup first
exec(open('report_modules/data_loader.py').read())
```

```{{python}}
# Test the specific module
exec(open('report_modules/{module_name}.py').read())
```

---
*Module test generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
'''
        
        mini_qmd = self.test_dir / f"test_{module_name}_report.qmd"
        with open(mini_qmd, 'w') as f:
            f.write(qmd_content)
        
        # Render with Quarto
        try:
            result = subprocess.run([
                'quarto', 'render', mini_qmd.name, '--to', 'html'
            ], capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                html_file = mini_qmd.with_suffix('.html')
                if html_file.exists():
                    print(f"✅ Mini-report rendered: {html_file}")
                    print(f"🌐 Open in browser: file://{html_file.absolute()}")
                    return True
            else:
                print(f"❌ Rendering failed: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error rendering: {e}")
            return False
    
    def test_all_modules(self, render: bool = False) -> dict:
        """Test all available modules"""
        
        print("🔄 Testing all modules...")
        results = {}
        
        for module_name in self.available_modules.keys():
            print(f"\n{'='*50}")
            success = self.test_module(module_name, render=False)  # Don't render individual tests
            results[module_name] = success
        
        # Summary
        print(f"\n{'='*50}")
        print("📊 TEST SUMMARY")
        print(f"{'='*50}")
        
        passed = sum(results.values())
        total = len(results)
        
        for module, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{module:20} {status}")
        
        print(f"\nOverall: {passed}/{total} modules passed")
        
        # If render requested and all passed, create full report
        if render and passed == total:
            print("\n🖨️ All tests passed, rendering full report...")
            return self._render_full_test_report()
        
        return results
    
    def _render_full_test_report(self) -> bool:
        """Render the full report after all tests pass"""
        
        # Find the main QMD file
        qmd_files = list(self.test_dir.glob("telco_*.qmd"))
        if not qmd_files:
            print("❌ No main QMD file found")
            return False
        
        main_qmd = qmd_files[0]
        
        try:
            result = subprocess.run([
                'quarto', 'render', main_qmd.name, '--to', 'html'
            ], capture_output=True, text=True, timeout=180)
            
            if result.returncode == 0:
                html_file = main_qmd.with_suffix('.html')
                if html_file.exists():
                    print(f"✅ Full report rendered: {html_file}")
                    print(f"🌐 Open in browser: file://{html_file.absolute()}")
                    return True
            else:
                print(f"❌ Full report rendering failed: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error rendering full report: {e}")
            return False
    
    def clean_test_environment(self):
        """Clean up the test environment"""
        
        if self.test_dir.exists():
            print(f"🧹 Cleaning test environment: {self.test_dir}")
            shutil.rmtree(self.test_dir)
            print("✅ Test environment cleaned")
        else:
            print("ℹ️ Test environment already clean")
    
    def list_modules(self):
        """List all available modules"""
        
        print("📋 Available modules for testing:")
        print()
        
        for module, description in self.available_modules.items():
            print(f"  --{module:20} {description}")
        
        print()
        print("Usage examples:")
        print("  python telco_test_module.py --data_loader")
        print("  python telco_test_module.py --income_statement --render")
        print("  python telco_test_module.py --all --render")


def main():
    """Main function with command line interface"""
    
    parser = argparse.ArgumentParser(
        description='Test individual Telco report modules',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python telco_test_module.py --list
  python telco_test_module.py --setup
  python telco_test_module.py --income_statement
  python telco_test_module.py --balance_sheet --render
  python telco_test_module.py --all --render
  python telco_test_module.py --clean
        '''
    )
    
    # Module selection arguments
    parser.add_argument('--data_loader', action='store_true',
                       help='Test setup and data loading module')
    parser.add_argument('--executive_summary', action='store_true',
                       help='Test executive summary module')
    parser.add_argument('--income_statement', action='store_true',
                       help='Test income statement module')
    parser.add_argument('--balance_sheet', action='store_true',
                       help='Test balance sheet module')
    parser.add_argument('--financial_ratios', action='store_true',
                       help='Test financial ratios module')
    parser.add_argument('--revenue_charts', action='store_true',
                       help='Test revenue charts module')
    parser.add_argument('--asset_charts', action='store_true',
                       help='Test asset charts module')
    parser.add_argument('--notes', action='store_true',
                       help='Test notes and methodology module')
    
    # Control arguments
    parser.add_argument('--all', action='store_true',
                       help='Test all modules')
    parser.add_argument('--render', action='store_true',
                       help='Render HTML report after testing')
    parser.add_argument('--list', action='store_true',
                       help='List all available modules')
    parser.add_argument('--clean', action='store_true',
                       help='Clean up test environment')
    parser.add_argument('--test-dir', type=str,
                       help='Custom test directory (default: test/module_tests)')
    
    args = parser.parse_args()
    
    # Determine test directory
    if args.test_dir:
        test_dir = Path(args.test_dir)
    else:
        test_dir = Path(__file__).parent / "module_tests"
    
    # Create tester
    tester = TelcoModuleTester(test_dir)
    
    # Handle special commands
    if args.list:
        tester.list_modules()
        return 0
    
    if args.clean:
        tester.clean_test_environment()
        return 0
    
    # Test specific modules
    modules_to_test = []
    for module in tester.available_modules.keys():
        if getattr(args, module):
            modules_to_test.append(module)
    
    if args.all:
        results = tester.test_all_modules(render=args.render)
        failed_modules = [module for module, success in results.items() if not success]
        return 0 if not failed_modules else 1
    
    elif modules_to_test:
        all_passed = True
        for module in modules_to_test:
            success = tester.test_module(module, render=args.render)
            if not success:
                all_passed = False
        return 0 if all_passed else 1
    
    else:
        print("No modules specified for testing.")
        print("Use --list to see available modules or --help for usage.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)