"""
Modular Report System

Orchestrates the generation of modular financial reports.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from financial_reporting.core.data_processor import ReportDataProcessor
from financial_reporting.reporting.template_generator import (
    create_telco_modules,
    create_telco_qmd_template,
)


class ModularReportOrchestrator:
    """Orchestrates the entire modular reporting process."""

    def __init__(self, company_name: str, report_year: str) -> None:
        self.company_name = company_name
        self.report_year = report_year
        self.processor = ReportDataProcessor(company_name, report_year)

    def generate_full_report(
        self,
        income_df: pl.DataFrame,
        balance_df: pl.DataFrame,
        output_dir: Path,
    ) -> Path:
        """Generate complete modular report and return path to the QMD file."""
        output_dir = Path(output_dir)

        print("🔄 Processing data...")
        income_result = self.processor.process_income_statement(income_df)
        balance_result = self.processor.process_balance_sheet(balance_df)

        self.processor.data["income_statement"] = income_result
        self.processor.data["balance_sheet"] = balance_result
        self.processor.save_processed_data(output_dir)

        print("📝 Generating templates...")
        create_telco_modules(output_dir)
        qmd_file = create_telco_qmd_template(output_dir, self.company_name, self.report_year)

        print(f"✅ Report generated: {qmd_file}")
        print(f"📁 Python modules in: {output_dir / 'report_modules'}")
        print(f"📊 Processed data in: {output_dir / 'processed_data'}")

        return qmd_file


__all__ = ["ModularReportOrchestrator"]
