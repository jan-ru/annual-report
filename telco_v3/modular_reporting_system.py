"""
modular_reporting_system — backward-compatibility shim.

All implementations have been moved to financial_reporting.reporting.
"""

from financial_reporting.core.data_processor import ReportDataProcessor
from financial_reporting.reporting.template_generator import ReportTemplateGenerator
from financial_reporting.reporting.modular_system import ModularReportOrchestrator

__all__ = [
    "ReportDataProcessor",
    "ReportTemplateGenerator",
    "ModularReportOrchestrator",
]
