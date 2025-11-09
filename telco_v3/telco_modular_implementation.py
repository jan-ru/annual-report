#!/usr/bin/env python3
"""
Telco B.V. Modular Financial Reporting Implementation
This replaces your existing telco_2024_report_generator.py with a modular approach
"""

import sys
import argparse
import subprocess
from pathlib import Path
import polars as pl
import os
from datetime import datetime
from version import __version__

# Import your existing XAF processing (if available)
try:
    from main_polars_pivot import XAFProcessor, create_comprehensive_income_statement, create_balance_sheet_mapping
    HAVE_XAF_PROCESSOR = True
except ImportError:
    print("⚠️ XAF processor not found, using sample data mode only")
    HAVE_XAF_PROCESSOR = False

# Import the modular system
from modular_reporting_system import ModularReportOrchestrator

# Define the column fix function directly here (from fix_column_names.py)
def fix_pivot_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Fix problematic column names created by Polars pivot operations
    Converts complex JSON-like names to simple readable names
    """
    import re
    
    # Get current column names
    current_columns = df.columns
    
    # Create mapping for renaming
    rename_mapping = {}
    
    for col in current_columns:
        if col in ['Categorie', 'Level0', 'Level1', 'Sort1', 'row_type', 'subtotal_level', 'Balance_Side', 'Note']:
            # Keep standard columns as-is
            continue
        
        # Handle complex JSON-like column names
        if '{"' in col and '"}' in col:
            # Extract meaningful parts
            # Example: '{"\"2024\"\",\"\"total\"\"}" -> "2024_total"
            
            # Remove outer braces and quotes
            cleaned = col.strip('{}')
            
            # Split by comma and clean each part
            parts = []
            for part in cleaned.split(','):
                # Remove quotes and extra characters
                clean_part = re.sub(r'["\s]', '', part)
                if clean_part:
                    parts.append(clean_part)
            
            # Create new column name
            if parts:
                new_name = '_'.join(parts)
            else:
                new_name = col  # Fallback to original
            
            rename_mapping[col] = new_name
    
    # Apply renaming if needed
    if rename_mapping:
        df = df.rename(rename_mapping)
        print(f"Renamed columns: {rename_mapping}")
    
    return df

class TelcoModularReportGenerator:
    """
    Modular version of Telco report generator
    Integrates with your existing XAF processing but uses modular templates
    """
    
    def __init__(self, company_name: str = "Telco B.V.", report_year: str = "2024"):
        self.company_name = company_name
        self.report_year = report_year
        self.orchestrator = ModularReportOrchestrator(company_name, report_year)
    
    def load_and_process_xaf(self, xaf_file_path: str) -> dict:
        """Load and process XAF file using existing logic"""
        
        if not HAVE_XAF_PROCESSOR:
            raise ImportError("XAF processor not available. Use --sample-data instead.")
        
        print(f"📁 Loading XAF file: {xaf_file_path}")
        
        # Use your existing XAF processing logic
        xaf_processor = XAFProcessor(xaf_file_path)
        xaf_processor.load_xaf_file()
        
        # Extract data using your existing methods
        trial_balance = xaf_processor.extract_trial_balance()
        transactions = xaf_processor.extract_transactions()
        chart_of_accounts = xaf_processor.extract_chart_of_accounts()
        
        # Process into financial statements using your existing logic
        income_statement_data, income_calculations, income_validations = create_comprehensive_income_statement(
            trial_balance, 
            include_calculations=True, 
            include_validations=True
        )
        
        balance_sheet_data = create_balance_sheet_mapping(trial_balance)
        
        return {
            'income_statement': income_statement_data,
            'balance_sheet': balance_sheet_data,
            'trial_balance': trial_balance,
            'calculations': income_calculations,
            'validations': income_validations
        }
    
    def create_telco_sample_data(self) -> dict:
        """Create sample data based on the 2023 Telco report structure"""
        
        print("📊 Creating Telco sample data based on 2023 annual report...")
        
        # Income statement data from the 2023 report
        income_data = pl.DataFrame({
            'Categorie': [
                'Netto-omzet',
                'Personeelskosten', 
                'Afschrijvingen',
                'Overige bedrijfskosten',
                'Bedrijfsresultaat',
                'Financiële baten en lasten',
                'Resultaat voor belastingen',
                'Belastingen',
                'Resultaat na belastingen',
                'Resultaat deelnemingen'
            ],
            '2024': [1100, -14922, 0, -155885, -169707, 13600, -156107, 0, -156107, 0],
            '2023': [0, -32650, -871, -11470, -44991, 10536, -34455, 1675, -32780, -436],
            'Note': ['Revenue from services', 'Wages and salaries', 'Depreciation', 'Other operating costs', 
                    'Calculated', 'Interest and investment income', 'Calculated', 'Corporate tax', 'Calculated', 'Share in results']
        })
        
        # Balance sheet data from the 2023 report
        balance_data = pl.DataFrame({
            'Categorie': [
                # ASSETS
                'Vastgoedbeleggingen',
                'Financiële vaste activa',
                'Vorderingen', 
                'Effecten',
                'Liquide middelen',
                'Totaal activa',
                # EQUITY & LIABILITIES  
                'Geplaatst kapitaal',
                'Overige reserves',
                'Eigen vermogen',
                'Kortlopende schulden',
                'Totaal passiva'
            ],
            '2024': [174222, 267607, 138091, 6379, 14653, 600952, 18151, 569901, 588052, 12900, 600952],
            '2023': [0, 672756, 35948, 39098, 298, 748100, 18151, 726008, 744159, 3941, 748100],
            'Note': [
                'Real estate investments', 'Financial fixed assets', 'Receivables', 'Securities', 'Cash and equivalents',
                'Total assets', 'Share capital', 'Other reserves', 'Total equity', 'Current liabilities', 'Total equity and liabilities'
            ]
        })
        
        return {
            'income_statement': income_data,
            'balance_sheet': balance_data,
            'trial_balance': None,
            'calculations': {},
            'validations': []
        }
    
    def generate_modular_report(self, output_dir: Path, use_sample_data: bool = False, xaf_file: str = None) -> Path:
        """Generate complete modular report"""
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        if use_sample_data:
            data = self.create_telco_sample_data()
        elif xaf_file:
            data = self.load_and_process_xaf(xaf_file)
        else:
            raise ValueError("Must specify either --sample-data or --xaf-file")
        
        # Generate modular report using the orchestrator
        qmd_file = self.orchestrator.generate_full_report(
            data['income_statement'], 
            data['balance_sheet'], 
            output_dir
        )
        
        return qmd_file
    
    def render_report(self, qmd_file: Path, formats: list = None) -> bool:
        """Render the report to specified formats"""
        
        if formats is None:
            formats = ['html']
        
        # Change to the output directory for rendering
        original_cwd = os.getcwd()
        os.chdir(qmd_file.parent)
        
        success = True
        
        try:
            for format_type in formats:
                print(f"🖨️ Rendering to {format_type.upper()}...")
                
                result = subprocess.run([
                    'quarto', 'render', qmd_file.name, '--to', format_type
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    output_file = qmd_file.with_suffix(f'.{format_type}')
                    if format_type == 'pdf':
                        output_file = qmd_file.with_suffix('.pdf')
                    
                    if output_file.exists():
                        print(f"✅ {format_type.upper()} rendering successful: {output_file}")
                        print(f"   📏 File size: {output_file.stat().st_size:,} bytes")
                    else:
                        print(f"⚠️ {format_type.upper()} file not found after rendering")
                        success = False
                else:
                    print(f"❌ {format_type.upper()} rendering failed!")
                    print("STDERR:", result.stderr[:500])
                    success = False
        
        except subprocess.TimeoutExpired:
            print(f"❌ Rendering timed out (>300 seconds)")
            success = False
        except Exception as e:
            print(f"❌ Error during rendering: {e}")
            success = False
        finally:
            os.chdir(original_cwd)
        
        return success


def main():
    """Main function with command line interface"""
    
    parser = argparse.ArgumentParser(
        description=f'Telco B.V. Modular Financial Report Generator v{__version__}',
        epilog=f'Financial Reporting System version {__version__}'
    )
    parser.add_argument('--version', action='version', 
                       version=f'%(prog)s {__version__}')
    parser.add_argument('--company-name', default='Telco B.V.', 
                       help='Company name for the report')
    parser.add_argument('--report-year', default='2024',
                       help='Report year')
    parser.add_argument('--sample-data', action='store_true',
                       help='Use sample data instead of XAF file')
    parser.add_argument('--xaf-file', type=str,
                       help='Path to XAF file to process')
    parser.add_argument('--output-dir', type=str, default='./reports/modular',
                       help='Output directory for generated reports')
    parser.add_argument('--render', action='store_true',
                       help='Automatically render the report after generation')
    parser.add_argument('--formats', nargs='+', default=['html'],
                       choices=['html', 'pdf', 'docx'],
                       help='Output formats for rendering')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug output')
    
    args = parser.parse_args()
    
    # Setup
    if args.debug:
        print("🐛 Debug mode enabled")
    
    print(f"📊 Financial Reporting System v{__version__}")
    print(f"🏢 Generating report for: {args.company_name}")
    print(f"📅 Report year: {args.report_year}")
    
    # Create generator
    generator = TelcoModularReportGenerator(args.company_name, args.report_year)
    
    # Generate report
    try:
        output_dir = Path(args.output_dir)
        print(f"📁 Output directory: {output_dir.absolute()}")
        
        qmd_file = generator.generate_modular_report(
            output_dir=output_dir,
            use_sample_data=args.sample_data,
            xaf_file=args.xaf_file
        )
        
        print(f"✅ Modular report generated successfully!")
        print(f"📄 QMD file: {qmd_file}")
        print(f"📁 Python modules: {output_dir / 'report_modules'}")
        print(f"📊 Processed data: {output_dir / 'processed_data'}")
        
        # Render if requested
        if args.render:
            print(f"🔄 Rendering to formats: {', '.join(args.formats)}")
            
            success = generator.render_report(qmd_file, args.formats)
            
            if success:
                print("🎉 Report rendering completed successfully!")
                
                # Show output files
                for format_type in args.formats:
                    if format_type == 'html':
                        output_file = qmd_file.with_suffix('.html')
                    elif format_type == 'pdf':
                        output_file = qmd_file.with_suffix('.pdf')
                    elif format_type == 'docx':
                        output_file = qmd_file.with_suffix('.docx')
                    
                    if output_file.exists():
                        print(f"📄 {format_type.upper()}: {output_file}")
                
                print(f"\n🌐 To view HTML report, open: {qmd_file.with_suffix('.html')}")
            else:
                print("🚨 Some rendering operations failed. Check output above.")
                return 1
        else:
            print(f"\n🔄 To render manually, run:")
            print(f"cd {output_dir}")
            print(f"quarto render {qmd_file.name}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1


def show_usage_examples():
    """Show usage examples"""
    
    print("📋 USAGE EXAMPLES:")
    print()
    print("# Generate report with sample data and render to HTML:")
    print("python telco_modular_implementation.py --sample-data --render")
    print()
    print("# Generate report from XAF file and render to PDF:")
    print("python telco_modular_implementation.py --xaf-file audit_2024.xaf --render --formats pdf")
    print()
    print("# Generate report with custom output directory:")
    print("python telco_modular_implementation.py --sample-data --output-dir ./custom_reports --render")
    print()
    print("# Generate multiple formats:")
    print("python telco_modular_implementation.py --sample-data --render --formats html pdf docx")
    print()
    print("🔧 MODULAR STRUCTURE CREATED:")
    print("reports/modular/")
    print("├── telco_bv_annual_report_2024.qmd    # Main template")
    print("├── report_modules/                     # Python modules")
    print("│   ├── setup.py                       # Data loading")
    print("│   ├── executive_summary.py           # Key highlights")
    print("│   ├── income_statement.py            # P&L display")
    print("│   ├── balance_sheet.py               # Balance sheet")
    print("│   ├── financial_ratios.py            # Calculations")
    print("│   ├── revenue_charts.py              # Revenue charts")
    print("│   ├── asset_charts.py                # Asset charts")
    print("│   └── notes.py                       # Methodology")
    print("└── processed_data/                    # Clean data files")
    print("    ├── income_statement.csv")
    print("    ├── balance_sheet.csv")
    print("    ├── calculations.json")
    print("    ├── metadata.json")
    print("    └── chart_data.json")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        show_usage_examples()
        print("\nAdd --help for full options, or --sample-data --render to get started")
    else:
        exit_code = main()
        sys.exit(exit_code)