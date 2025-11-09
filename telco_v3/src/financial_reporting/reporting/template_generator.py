"""
Template Generator Module

Generates Quarto templates and Python modules for reports.

This module currently imports from the legacy modular_reporting_system module.
TODO: Refactor to standalone implementation.
"""

# Temporary: Import from legacy module
import sys
from pathlib import Path

# Add root to path to import legacy modules
root_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from modular_reporting_system import (
    ReportTemplateGenerator as LegacyTemplateGenerator,
    ReportDataProcessor,
)


class ReportTemplateGenerator(LegacyTemplateGenerator):
    """
    Wrapper around legacy ReportTemplateGenerator.
    Maintains compatibility while allowing gradual refactoring.
    """
    pass


__all__ = ["ReportTemplateGenerator", "ReportDataProcessor"]
