#!/usr/bin/env python3
"""
Test script for income_statement module
Generated automatically by telco_test_module.py
"""

import sys
import traceback
from pathlib import Path

def test_income_statement_module():
    """Test the income_statement module"""
    
    print(f"Testing income_statement module...")
    
    try:
        # First run setup to ensure data is loaded
        print("Loading setup module...")
        exec(open('report_modules/data_loader.py').read())
        print("✅ Setup module loaded successfully")
        
        # Then run the specific module
        print(f"Loading income_statement module...")
        exec(open('report_modules/income_statement.py').read())  
        print(f"✅ income_statement module executed successfully")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ Module file not found: {e}")
        return False
    except Exception as e:
        print(f"❌ Error in income_statement module: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_income_statement_module()
    sys.exit(0 if success else 1)
