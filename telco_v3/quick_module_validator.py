#!/usr/bin/env python3
"""
quick_module_validator — backward-compatibility shim.

All implementations have been moved to financial_reporting.cli.validator.
"""

from financial_reporting.cli.validator import quick_validate_module, main

__all__ = ["quick_validate_module", "main"]

if __name__ == "__main__":
    import sys
    sys.exit(main())
