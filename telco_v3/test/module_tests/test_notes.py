#!/usr/bin/env python3
"""
Test script for notes module
Generated automatically by telco_test_module.py
"""

import sys
import traceback
from pathlib import Path

def test_notes_module():
    """Test the notes module"""
    
    print(f"Testing notes module...")
    
    try:
        # First run setup to ensure data is loaded
        print("Loading setup module...")
        exec(open('report_modules/data_loader.py').read())
        print("✅ Setup module loaded successfully")
        
        # Then run the specific module
        print(f"Loading notes module...")
        exec(open('report_modules/notes.py').read())  
        print(f"✅ notes module executed successfully")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ Module file not found: {e}")
        return False
    except Exception as e:
        print(f"❌ Error in notes module: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_notes_module()
    sys.exit(0 if success else 1)
