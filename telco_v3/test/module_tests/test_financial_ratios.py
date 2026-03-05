#!/usr/bin/env python3
"""
Test script for financial_ratios module
Generated automatically by telco_test_module.py
"""

import sys
import traceback
from pathlib import Path

def test_financial_ratios_module():
    """Test the financial_ratios module"""
    
    print(f"Testing financial_ratios module...")
    
    try:
        # First run setup to ensure data is loaded
        print("Loading setup module...")
        exec(open('report_modules/data_loader.py').read())
        print("✅ Setup module loaded successfully")
        
        # Then run the specific module
        print(f"Loading financial_ratios module...")
        exec(open('report_modules/financial_ratios.py').read())  
        print(f"✅ financial_ratios module executed successfully")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ Module file not found: {e}")
        return False
    except Exception as e:
        print(f"❌ Error in financial_ratios module: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_financial_ratios_module()
    sys.exit(0 if success else 1)
