"""
Telco Report CLI

Main command-line interface for generating Telco B.V. financial reports.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import polars as pl

try:
    from version import __version__
except ImportError:
    __version__ = "0.3.0"

from financial_reporting.reporting.modular_system import ModularReportOrchestrator

try:
    from financial_reporting.core.xaf_processor import XAFProcessor
    from financial_reporting.core.data_processor import (
        create_comprehensive_income_statement,
        create_balance_sheet_mapping,
    )

    HAVE_XAF_PROCESSOR = True
except ImportError:
    HAVE_XAF_PROCESSOR = False


class TelcoModularReportGenerator:
    """Generate Telco B.V. modular financial reports."""

    def __init__(
        self, company_name: str = "Telco B.V.", report_year: str = "2024"
    ) -> None:
        self.company_name = company_name
        self.report_year = report_year
        self.orchestrator = ModularReportOrchestrator(company_name, report_year)

    def load_and_process_xaf(self, xaf_file_path: str) -> dict:
        if not HAVE_XAF_PROCESSOR:
            raise ImportError("XAF processor not available. Use --sample-data instead.")

        print(f"📁 Loading XAF file: {xaf_file_path}")
        xaf_processor = XAFProcessor(xaf_file_path)
        xaf_processor.load_xaf_file()

        trial_balance = xaf_processor.extract_trial_balance()
        income_statement_data, income_calculations, income_validations = (
            create_comprehensive_income_statement(
                trial_balance, include_calculations=True, include_validations=True
            )
        )
        balance_sheet_data = create_balance_sheet_mapping()

        return {
            "income_statement": income_statement_data,
            "balance_sheet": balance_sheet_data,
            "trial_balance": trial_balance,
            "calculations": income_calculations,
            "validations": income_validations,
        }

    def create_telco_sample_data(self) -> dict:
        """Create sample data based on the 2023 Telco report structure."""
        print("📊 Creating Telco sample data based on 2023 annual report...")

        income_data = pl.DataFrame(
            {
                "Categorie": [
                    "Netto-omzet",
                    "Personeelskosten",
                    "Afschrijvingen",
                    "Overige bedrijfskosten",
                    "Bedrijfsresultaat",
                    "Financiële baten en lasten",
                    "Resultaat voor belastingen",
                    "Belastingen",
                    "Resultaat na belastingen",
                    "Resultaat deelnemingen",
                ],
                "2024": [1100, -14922, 0, -155885, -169707, 13600, -156107, 0, -156107, 0],
                "2023": [0, -32650, -871, -11470, -44991, 10536, -34455, 1675, -32780, -436],
                "Note": [
                    "Revenue from services",
                    "Wages and salaries",
                    "Depreciation",
                    "Other operating costs",
                    "Calculated",
                    "Interest and investment income",
                    "Calculated",
                    "Corporate tax",
                    "Calculated",
                    "Share in results",
                ],
            }
        )

        balance_data = pl.DataFrame(
            {
                "Categorie": [
                    "Vastgoedbeleggingen",
                    "Financiële vaste activa",
                    "Vorderingen",
                    "Effecten",
                    "Liquide middelen",
                    "Totaal activa",
                    "Geplaatst kapitaal",
                    "Overige reserves",
                    "Eigen vermogen",
                    "Kortlopende schulden",
                    "Totaal passiva",
                ],
                "2024": [174222, 267607, 138091, 6379, 14653, 600952, 18151, 569901, 588052, 12900, 600952],
                "2023": [0, 672756, 35948, 39098, 298, 748100, 18151, 726008, 744159, 3941, 748100],
                "Note": [
                    "Real estate investments",
                    "Financial fixed assets",
                    "Receivables",
                    "Securities",
                    "Cash and equivalents",
                    "Total assets",
                    "Share capital",
                    "Other reserves",
                    "Total equity",
                    "Current liabilities",
                    "Total equity and liabilities",
                ],
            }
        )

        return {
            "income_statement": income_data,
            "balance_sheet": balance_data,
            "trial_balance": None,
            "calculations": {},
            "validations": [],
        }

    def generate_modular_report(
        self,
        output_dir: Path,
        use_sample_data: bool = False,
        xaf_file: str | None = None,
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)

        if use_sample_data:
            data = self.create_telco_sample_data()
        elif xaf_file:
            data = self.load_and_process_xaf(xaf_file)
        else:
            raise ValueError("Must specify either --sample-data or --xaf-file")

        return self.orchestrator.generate_full_report(
            data["income_statement"], data["balance_sheet"], output_dir
        )

    def render_report(self, qmd_file: Path, formats: list[str] | None = None) -> bool:
        if formats is None:
            formats = ["html"]

        original_cwd = os.getcwd()
        os.chdir(qmd_file.parent)
        success = True

        try:
            for fmt in formats:
                print(f"🖨️ Rendering to {fmt.upper()}...")
                result = subprocess.run(
                    ["quarto", "render", qmd_file.name, "--to", fmt],
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if result.returncode == 0:
                    output_file = qmd_file.with_suffix(f".{fmt}")
                    if output_file.exists():
                        print(f"✅ {fmt.upper()}: {output_file} ({output_file.stat().st_size:,} bytes)")
                    else:
                        print(f"⚠️ {fmt.upper()} file not found after rendering")
                        success = False
                else:
                    print(f"❌ {fmt.upper()} rendering failed!")
                    print("STDERR:", result.stderr[:500])
                    success = False

        except subprocess.TimeoutExpired:
            print("❌ Rendering timed out (>300 seconds)")
            success = False
        except Exception as e:
            print(f"❌ Error during rendering: {e}")
            success = False
        finally:
            os.chdir(original_cwd)

        return success


def main() -> int:
    parser = argparse.ArgumentParser(
        description=f"Telco B.V. Modular Financial Report Generator v{__version__}",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--company-name", default="Telco B.V.")
    parser.add_argument("--report-year", default="2024")
    parser.add_argument("--sample-data", action="store_true")
    parser.add_argument("--xaf-file", type=str)
    parser.add_argument("--output-dir", type=str, default="./reports/modular")
    parser.add_argument("--render", action="store_true")
    parser.add_argument(
        "--formats", nargs="+", default=["html"], choices=["html", "pdf", "docx"]
    )
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    print(f"📊 Financial Reporting System v{__version__}")
    print(f"🏢 Generating report for: {args.company_name}")
    print(f"📅 Report year: {args.report_year}")

    generator = TelcoModularReportGenerator(args.company_name, args.report_year)

    try:
        output_dir = Path(args.output_dir)
        qmd_file = generator.generate_modular_report(
            output_dir=output_dir,
            use_sample_data=args.sample_data,
            xaf_file=args.xaf_file,
        )
        print("✅ Report generated!")
        print(f"📄 QMD: {qmd_file}")

        if args.render:
            success = generator.render_report(qmd_file, args.formats)
            return 0 if success else 1
        else:
            print(f"\nTo render: cd {output_dir} && quarto render {qmd_file.name}")
        return 0

    except Exception as e:
        print(f"❌ Error: {e}")
        if args.debug:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
