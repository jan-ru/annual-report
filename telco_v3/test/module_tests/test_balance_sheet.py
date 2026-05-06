#!/usr/bin/env python3
"""
Test script for balance_sheet module
Generated automatically by telco_test_module.py
"""

import sys
import traceback
from pathlib import Path

def test_balance_sheet_module():
    """Test the balance_sheet module"""
    
    print(f"Testing balance_sheet module...")
    
    try:
        # First run setup to ensure data is loaded
        print("Loading setup module...")
        exec(open('report_modules/data_loader.py').read())
        print("✅ Setup module loaded successfully")
        
        # Then run the specific module
        print(f"Loading balance_sheet module...")
        exec(open('report_modules/balance_sheet.py').read())  
        print(f"✅ balance_sheet module executed successfully")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ Module file not found: {e}")
        return False
    except Exception as e:
        print(f"❌ Error in balance_sheet module: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_balance_sheet_module()
    sys.exit(0 if success else 1)
