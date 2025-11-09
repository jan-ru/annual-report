"""
CLI Module

Command-line interface tools for the financial reporting system.
"""

from .telco_report import main as telco_report_main, TelcoModularReportGenerator
from .validator import main as validator_main

__all__ = [
    "telco_report_main",
    "TelcoModularReportGenerator",
    "validator_main",
]
