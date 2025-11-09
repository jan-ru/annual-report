#!/usr/bin/env python3
"""
Test script for revenue_charts module
Generated automatically by telco_test_module.py
"""

import sys
import traceback
from pathlib import Path

def test_revenue_charts_module():
    """Test the revenue_charts module"""
    
    print(f"Testing revenue_charts module...")
    
    try:
        # First run setup to ensure data is loaded
        print("Loading setup module...")
        exec(open('report_modules/setup.py').read())
        print("✅ Setup module loaded successfully")
        
        # Then run the specific module
        print(f"Loading revenue_charts module...")
        exec(open('report_modules/revenue_charts.py').read())  
        print(f"✅ revenue_charts module executed successfully")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ Module file not found: {e}")
        return False
    except Exception as e:
        print(f"❌ Error in revenue_charts module: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_revenue_charts_module()
    sys.exit(0 if success else 1)
