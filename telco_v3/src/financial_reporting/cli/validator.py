"""
Module Validator CLI

Quick validation tool for report modules.

This module currently imports from the legacy quick_module_validator module.
TODO: Refactor to standalone implementation.
"""

# Temporary: Import from legacy module
import sys
from pathlib import Path

# Add root to path to import legacy modules
root_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from quick_module_validator import (
    quick_validate_module,
    main,
)


__all__ = ["quick_validate_module", "main"]
