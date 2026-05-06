"""
telco_enhanced_modules — backward-compatibility shim.

All implementations have been moved to financial_reporting.reporting.template_generator.
"""

from financial_reporting.reporting.template_generator import (
    create_telco_modules,
    create_telco_qmd_template,
    ReportTemplateGenerator,
)

__all__ = [
    "create_telco_modules",
    "create_telco_qmd_template",
    "ReportTemplateGenerator",
]
