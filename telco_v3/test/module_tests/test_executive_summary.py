#!/usr/bin/env python3
"""
Test script for executive_summary module
Generated automatically by telco_test_module.py
"""

import sys
import traceback
from pathlib import Path

def test_executive_summary_module():
    """Test the executive_summary module"""
    
    print(f"Testing executive_summary module...")
    
    try:
        # First run setup to ensure data is loaded
        print("Loading setup module...")
        exec(open('report_modules/data_loader.py').read())
        print("✅ Setup module loaded successfully")
        
        # Then run the specific module
        print(f"Loading executive_summary module...")
        exec(open('report_modules/executive_summary.py').read())  
        print(f"✅ executive_summary module executed successfully")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ Module file not found: {e}")
        return False
    except Exception as e:
        print(f"❌ Error in executive_summary module: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_executive_summary_module()
    sys.exit(0 if success else 1)
