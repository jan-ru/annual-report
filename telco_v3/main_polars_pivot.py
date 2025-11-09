"""
Complete Financial Reporting System - Updated Polars Version
Fixes deprecation warnings and adds XAF file processing capabilities

Version: 0.2.0
"""

import polars as pl
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import re

try:
    from version import __version__
except ImportError:
    __version__ = "0.2.0"

# ==========================================
# 1. XAF FILE PROCESSING MODULE
# ==========================================

class XAFProcessor:
    """Process XAF (XML Audit File) format for Dutch accounting data."""
    
    def __init__(self, xaf_file_path: str):
        self.xaf_file_path = Path(xaf_file_path)
        self.tree = None
        self.root = None
        self.namespace = {}
        
    def load_xaf_file(self) -> bool:
        """Load and parse XAF file."""
        try:
            self.tree = ET.parse(self.xaf_file_path)
            self.root = self.tree.getroot()
            
            # Extract namespace
            if self.root.tag.startswith('{'):
                namespace_uri = self.root.tag.split('}')[0][1:]
                self.namespace = {'xaf': namespace_uri}
            
            return True
        except Exception as e:
            print(f"Error loading XAF file: {e}")
            return False
    
    def extract_chart_of_accounts(self) -> pl.DataFrame:
        """Extract chart of accounts from XAF file."""
        accounts = []
        
        # Find all account elements
        account_elements = self.root.findall('.//xaf:account', self.namespace) if self.namespace else self.root.findall('.//account')
        
        for account in account_elements:
            account_data = {
                'account_id': self._get_element_text(account, 'accountID'),
                'account_desc': self._get_element_text(account, 'accountDesc'),
                'account_type': self._get_element_text(account, 'accountType'),
                'account_class': self._get_element_text(account, 'accountClass'),
                'opening_balance_debit': self._get_element_float(account, 'openingBalanceDebit'),
                'opening_balance_credit': self._get_element_float(account, 'openingBalanceCredit')
            }
            accounts.append(account_data)
        
        return pl.DataFrame(accounts)
    
    def extract_transactions(self) -> pl.DataFrame:
        """Extract all transactions from XAF file."""
        transactions = []
        
        # Find all transaction elements
        transaction_elements = self.root.findall('.//xaf:transaction', self.namespace) if self.namespace else self.root.findall('.//transaction')
        
        for transaction in transaction_elements:
            # Get transaction header info
            trans_id = self._get_element_text(transaction, 'transactionID')
            trans_date = self._get_element_text(transaction, 'transactionDate')
            description = self._get_element_text(transaction, 'description')
            
            # Process transaction lines
            lines = transaction.findall('.//xaf:line', self.namespace) if self.namespace else transaction.findall('.//line')
            
            for line in lines:
                line_data = {
                    'transaction_id': trans_id,
                    'transaction_date': trans_date,
                    'description': description,
                    'line_number': self._get_element_text(line, 'lineNumber'),
                    'account_id': self._get_element_text(line, 'accountID'),
                    'debit_amount': self._get_element_float(line, 'debitAmount'),
                    'credit_amount': self._get_element_float(line, 'creditAmount'),
                    'line_description': self._get_element_text(line, 'lineDescription')
                }
                transactions.append(line_data)
        
        return pl.DataFrame(transactions)
    
    def extract_trial_balance(self) -> pl.DataFrame:
        """Extract trial balance data from XAF file."""
        trial_balance = []
        
        # Find trial balance elements
        tb_elements = self.root.findall('.//xaf:trialBalance', self.namespace) if self.namespace else self.root.findall('.//trialBalance')
        
        for tb_line in tb_elements:
            tb_data = {
                'account_id': self._get_element_text(tb_line, 'accountID'),
                'account_desc': self._get_element_text(tb_line, 'accountDesc'),
                'opening_balance_debit': self._get_element_float(tb_line, 'openingBalanceDebit'),
                'opening_balance_credit': self._get_element_float(tb_line, 'openingBalanceCredit'),
                'turnover_debit': self._get_element_float(tb_line, 'turnoverDebit'),
                'turnover_credit': self._get_element_float(tb_line, 'turnoverCredit'),
                'closing_balance_debit': self._get_element_float(tb_line, 'closingBalanceDebit'),
                'closing_balance_credit': self._get_element_float(tb_line, 'closingBalanceCredit')
            }
            trial_balance.append(tb_data)
        
        return pl.DataFrame(trial_balance)
    
    def _get_element_text(self, parent, tag_name: str) -> Optional[str]:
        """Get text content of an XML element."""
        element = parent.find(f'.//xaf:{tag_name}', self.namespace) if self.namespace else parent.find(f'.//{tag_name}')
        return element.text if element is not None else None
    
    def _get_element_float(self, parent, tag_name: str) -> Optional[float]:
        """Get float value of an XML element."""
        text = self._get_element_text(parent, tag_name)
        try:
            return float(text) if text else None
        except (ValueError, TypeError):
            return None

# ==========================================
# 2. FORMULAS - Via Hierarchy Mapping Table
# ==========================================

def create_income_statement_mapping():
    """Define the hierarchical structure for income statement (Dutch GAAP)."""
    return pl.DataFrame({
        "Categorie": [
            "Netto-omzet", "Overige bedrijfsopbrengsten", 
            "Personeelskosten", "Afschrijvingen", "Overige bedrijfskosten",
            "Rentebaten", "Rentelasten", "Waardeveranderingen effecten",
            "Vennootschapsbelasting", "Resultaat deelnemingen"
        ],
        "Level0": [
            "Bedrijfsopbrengsten", "Bedrijfsopbrengsten",
            "Bedrijfskosten", "Bedrijfskosten", "Bedrijfskosten", 
            "Financiële baten en lasten", "Financiële baten en lasten", "Financiële baten en lasten",
            "Belastingen en deelnemingen", "Belastingen en deelnemingen"
        ],
        "Level1": [
            "Omzet", "Omzet",
            "Personeelskosten", "Afschrijvingen", "Overige kosten",
            "Rentebaten", "Rentelasten", "Overige financiële resultaten", 
            "Belastingen", "Resultaat deelnemingen"
        ],
        "Sort1": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
        "Display_Order": list(range(1, 11)),
        "In_Subtotal": [True] * 10
    })

def create_balance_sheet_mapping():
    """Define the hierarchical structure for balance sheet (Dutch GAAP)."""
    return pl.DataFrame({
        "Categorie": [
            # ACTIVA
            "Materiële vaste activa", "Financiële vaste activa", "Vastgoedbeleggingen",
            "Vorderingen", "Effecten", "Liquide middelen",
            # PASSIVA  
            "Eigen vermogen", "Voorzieningen", "Langlopende schulden", "Kortlopende schulden"
        ],
        "Level0": [
            "Vaste activa", "Vaste activa", "Vaste activa",
            "Vlottende activa", "Vlottende activa", "Vlottende activa",
            "Eigen vermogen", "Voorzieningen", "Vreemd vermogen langlopend", "Vreemd vermogen kortlopend"
        ],
        "Level1": [
            "Materiële vaste activa", "Financiële vaste activa", "Vastgoedbeleggingen",
            "Vorderingen", "Effecten", "Liquide middelen",
            "Eigen vermogen", "Voorzieningen", "Langlopende schulden", "Kortlopende schulden"
        ],
        "Sort1": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
        "Display_Order": list(range(1, 11)),
        "In_Subtotal": [True] * 10,
        "Balance_Side": ["Activa"] * 6 + ["Passiva"] * 4
    })

# ==========================================
# 3. CALCULATIONS - Derived Metrics Module
# ==========================================

@dataclass
class CalculationResult:
    name: str
    value: float
    formula: str
    year: str
    category: str = "calculated"

class FinancialCalculations:
    @staticmethod
    def calculate_operating_margin(operating_income: float, revenue: float) -> Optional[float]:
        return (operating_income / revenue * 100) if revenue != 0 else None
    
    @staticmethod
    def calculate_ebitda(operating_income: float, depreciation: float) -> float:
        return operating_income + depreciation
    
    @staticmethod
    def calculate_revenue_growth(current_revenue: float, previous_revenue: float) -> Optional[float]:
        return ((current_revenue - previous_revenue) / previous_revenue * 100) if previous_revenue != 0 else None
    
    @staticmethod
    def calculate_current_ratio(current_assets: float, current_liabilities: float) -> Optional[float]:
        return current_assets / current_liabilities if current_liabilities != 0 else None
    
    @staticmethod
    def calculate_debt_to_equity(total_debt: float, total_equity: float) -> Optional[float]:
        return total_debt / total_equity if total_equity != 0 else None

# ==========================================
# 4. CORE POLARS PROCESSING (FIXED DEPRECATION WARNINGS)
# ==========================================

def prepare_data_for_pivot(raw_data: pl.DataFrame, mapping: pl.DataFrame, total_only_categories=None):
    """Prepare data in long format for pivot operations using Polars (updated and fixed)."""
    if total_only_categories is None:
        total_only_categories = []
    
    # Join with mapping
    combined = raw_data.join(mapping, on='Categorie', how='left').filter(
        pl.col("Level0").is_not_null()
    )
    
    # Identify year columns
    excluded_cols = ['Categorie', 'Note', 'Level0', 'Level1', 'Sort1', 'Display_Order', 'In_Subtotal']
    if 'Balance_Side' in combined.columns:
        excluded_cols.append('Balance_Side')
    
    year_columns = [col for col in combined.columns if col not in excluded_cols]
    
    # Check if data has detail/total format
    detail_cols = [col for col in year_columns if col.endswith('_detail')]
    total_cols = [col for col in year_columns if col.endswith('_total')]
    
    if detail_cols and total_cols:
        # FIXED: Ensure consistent schema for concatenation
        base_years = [col.replace('_detail', '') for col in detail_cols 
                     if f"{col.replace('_detail', '')}_total" in total_cols]
        
        # Get common columns for unpivot
        base_columns = ['Categorie', 'Level0', 'Level1', 'Sort1']
        if 'Balance_Side' in combined.columns:
            base_columns.append('Balance_Side')
        
        # Unpivot detail columns with explicit type casting
        detail_melted = combined.select(
            base_columns + detail_cols
        ).unpivot(
            index=base_columns,
            on=detail_cols,
            variable_name='Year_Type',
            value_name='Amount'
        ).with_columns([
            pl.col('Year_Type').str.replace('_detail', '').alias('Year'),
            pl.lit('detail').alias('Type'),
            pl.col('Amount').cast(pl.Float64, strict=False)  # Ensure consistent type
        ]).drop('Year_Type')
        
        # Unpivot total columns with explicit type casting
        total_melted = combined.select(
            base_columns + total_cols
        ).unpivot(
            index=base_columns,
            on=total_cols,
            variable_name='Year_Type',
            value_name='Amount'
        ).with_columns([
            pl.col('Year_Type').str.replace('_total', '').alias('Year'),
            pl.lit('total').alias('Type'),
            pl.col('Amount').cast(pl.Float64, strict=False)  # Ensure consistent type
        ]).drop('Year_Type')
        
        # Ensure both DataFrames have identical schemas before concatenation
        # Get all columns and ensure they exist in both DataFrames with same types
        all_columns = set(detail_melted.columns) | set(total_melted.columns)
        
        # Add missing columns to each DataFrame with null values of correct type
        for col in all_columns:
            if col not in detail_melted.columns:
                if col == 'Balance_Side':
                    detail_melted = detail_melted.with_columns(pl.lit(None).cast(pl.String).alias(col))
                else:
                    detail_melted = detail_melted.with_columns(pl.lit(None).cast(pl.String).alias(col))
            
            if col not in total_melted.columns:
                if col == 'Balance_Side':
                    total_melted = total_melted.with_columns(pl.lit(None).cast(pl.String).alias(col))
                else:
                    total_melted = total_melted.with_columns(pl.lit(None).cast(pl.String).alias(col))
        
        # Ensure column order is the same
        column_order = sorted(all_columns)
        detail_melted = detail_melted.select(column_order)
        total_melted = total_melted.select(column_order)
        
        # Now concatenate with aligned schemas
        long_data = pl.concat([detail_melted, total_melted], how="vertical")
        
        # Apply total_only logic using Polars expressions
        long_data = long_data.with_columns([
            pl.when(
                pl.col('Categorie').is_in(total_only_categories) & 
                (pl.col('Type') == 'detail') & 
                pl.col('Amount').is_not_null()
            ).then(pl.lit('total')).otherwise(pl.col('Type')).alias('Type'),
            
            pl.when(
                pl.col('Categorie').is_in(total_only_categories) & 
                (pl.col('Type') == 'detail')
            ).then(pl.lit(None).cast(pl.Float64)).otherwise(pl.col('Amount')).alias('Amount')
        ])
        
    else:
        # Handle case where we only have direct year columns (no detail/total split)
        base_columns = ['Categorie', 'Level0', 'Level1', 'Sort1']
        if 'Balance_Side' in combined.columns:
            base_columns.append('Balance_Side')
        
        # Unpivot year columns directly
        long_data = combined.select(
            base_columns + year_columns
        ).unpivot(
            index=base_columns,
            on=year_columns,
            variable_name='Year',
            value_name='Amount'
        ).with_columns([
            pl.when(pl.col('Categorie').is_in(total_only_categories))
            .then(pl.lit('total'))
            .otherwise(pl.lit('detail'))
            .alias('Type'),
            pl.col('Amount').cast(pl.Float64, strict=False)  # Ensure consistent type
        ])
    
    return long_data.filter(pl.col('Amount').is_not_null())


def calculate_subtotals_with_pivot(long_data: pl.DataFrame, total_only_categories=None):
    """Use Polars pivot to calculate hierarchical subtotals efficiently (fixed schema issues)."""
    if total_only_categories is None:
        total_only_categories = []
    
    # Filter detail data for subtotal calculations
    detail_data = long_data.filter(
        ~pl.col('Categorie').is_in(total_only_categories) & 
        (pl.col('Type') == 'detail')
    )
    
    subtotal_frames = []
    
    if detail_data.height > 0:
        # Get all columns that should be present
        base_columns = ['Level0', 'Level1', 'Sort1', 'Type', 'Amount']
        if 'Balance_Side' in detail_data.columns:
            base_columns.append('Balance_Side')
        
        # Level0 subtotals using Polars group_by
        level0_agg = detail_data.group_by(['Level0', 'Year']).agg([
            pl.sum('Amount').alias('Amount')
        ]).with_columns([
            pl.col('Level0').alias('Categorie'),
            pl.lit(None).cast(pl.String).alias('Level1'),
            pl.lit(None).cast(pl.String).alias('Sort1'),
            pl.lit('total').alias('Type'),
            pl.lit('subtotal').alias('row_type'),
            pl.lit('Level0').alias('subtotal_level')
        ])
        
        # Add Balance_Side if it exists in original data
        if 'Balance_Side' in detail_data.columns:
            # Get Balance_Side from the original detail data by Level0
            balance_side_mapping = detail_data.group_by('Level0').agg([
                pl.first('Balance_Side').alias('Balance_Side')
            ])
            level0_agg = level0_agg.join(balance_side_mapping, on='Level0', how='left')
        else:
            level0_agg = level0_agg.with_columns(pl.lit(None).cast(pl.String).alias('Balance_Side'))
        
        subtotal_frames.append(level0_agg)
        
        # Level1 subtotals using Polars group_by
        level1_detail = detail_data.filter(pl.col('Level1').is_not_null())
        if level1_detail.height > 0:
            level1_agg = level1_detail.group_by([
                'Level0', 'Level1', 'Year'
            ]).agg([
                pl.sum('Amount').alias('Amount')
            ]).with_columns([
                pl.col('Level1').alias('Categorie'),
                pl.lit(None).cast(pl.String).alias('Sort1'),
                pl.lit('total').alias('Type'),
                pl.lit('subtotal').alias('row_type'),
                pl.lit('Level1').alias('subtotal_level')
            ])
            
            # Add Balance_Side if it exists
            if 'Balance_Side' in detail_data.columns:
                balance_side_mapping = level1_detail.group_by(['Level0', 'Level1']).agg([
                    pl.first('Balance_Side').alias('Balance_Side')
                ])
                level1_agg = level1_agg.join(balance_side_mapping, on=['Level0', 'Level1'], how='left')
            else:
                level1_agg = level1_agg.with_columns(pl.lit(None).cast(pl.String).alias('Balance_Side'))
            
            subtotal_frames.append(level1_agg)
    
    return subtotal_frames


def calculate_base_subtotals_with_pivot(raw_data: pl.DataFrame, mapping: pl.DataFrame, total_only_categories=None):
    """Base function using Polars pivot operations for efficient aggregation (fixed column names)."""
    
    # Prepare long format data
    long_data = prepare_data_for_pivot(raw_data, mapping, total_only_categories)
    
    # Add original data markers
    long_data = long_data.with_columns([
        pl.lit('original').alias('row_type'),
        pl.lit(None).cast(pl.String).alias('subtotal_level')
    ])
    
    # Calculate subtotals using pivot-like operations
    subtotal_frames = calculate_subtotals_with_pivot(long_data, total_only_categories)
    
    # Ensure all frames have consistent schemas before concatenation
    all_frames = [long_data] + subtotal_frames
    
    if len(all_frames) > 1:
        # Get all unique columns across all DataFrames
        all_columns = set()
        for frame in all_frames:
            all_columns.update(frame.columns)
        
        # Align schemas by adding missing columns with appropriate null types
        aligned_frames = []
        for frame in all_frames:
            aligned_frame = frame
            
            for col in all_columns:
                if col not in frame.columns:
                    # Determine appropriate null type based on column name
                    if col in ['Amount']:
                        aligned_frame = aligned_frame.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
                    else:
                        aligned_frame = aligned_frame.with_columns(pl.lit(None).cast(pl.String).alias(col))
            
            # Ensure column order is consistent
            column_order = sorted(all_columns)
            aligned_frame = aligned_frame.select(column_order)
            aligned_frames.append(aligned_frame)
        
        # Now concatenate with aligned schemas
        combined_long = pl.concat(aligned_frames, how="vertical")
    else:
        combined_long = long_data
    
    # Create pivot table with error handling
    try:
        # FIXED: Use 'on' instead of 'columns' parameter and handle schema properly
        wide_result = combined_long.pivot(
            values='Amount',
            index=['Categorie', 'Level0', 'Level1', 'Sort1', 'row_type', 'subtotal_level'] + 
                   (['Balance_Side'] if 'Balance_Side' in combined_long.columns else []),
            on=['Year', 'Type'],  # Fixed: changed from 'columns' to 'on'
            aggregate_function='first'
        )
    except Exception as e:
        print(f"Pivot operation failed: {e}")
        print("Available columns:", combined_long.columns)
        print("Sample data:", combined_long.head(3))
        raise
    
    # FIXED: Better column naming approach
    # Get the year columns that were created by pivot
    year_columns = []
    rename_map = {}
    
    for col in wide_result.columns:
        if isinstance(col, tuple) and len(col) == 2:
            year, type_suffix = col
            # Create clean column names
            if type_suffix == 'total':
                new_name = f"{year}_total"
            elif type_suffix == 'detail':
                new_name = f"{year}_detail"
            else:
                new_name = f"{year}_{type_suffix}"
            
            rename_map[col] = new_name
            
            # Track unique years
            if year not in year_columns:
                year_columns.append(year)
    
    # Apply the rename mapping
    if rename_map:
        wide_result = wide_result.rename(rename_map)
    
    # Add Note column if not present and sort
    if 'Note' not in wide_result.columns:
        wide_result = wide_result.with_columns([
            pl.lit(None).cast(pl.String).alias('Note')
        ])
    
    # Sort with null handling
    sort_columns = []
    for col in ['Level0', 'Level1', 'Sort1', 'row_type', 'Categorie']:
        if col in wide_result.columns:
            sort_columns.append(col)
    
    if sort_columns:
        wide_result = wide_result.sort(sort_columns)
    
    return wide_result, year_columns


# ==========================================
# 5. XAF TO FINANCIAL STATEMENTS CONVERTER
# ==========================================

class XAFToFinancialStatements:
    """Convert XAF data to standardized financial statements."""
    
    def __init__(self, xaf_processor: XAFProcessor, account_mapping: pl.DataFrame):
        self.xaf_processor = xaf_processor
        self.account_mapping = account_mapping
        self.trial_balance = None
        self.accounts = None
        
    def load_data(self):
        """Load all necessary data from XAF file."""
        if not self.xaf_processor.load_xaf_file():
            raise ValueError("Failed to load XAF file")
        
        self.trial_balance = self.xaf_processor.extract_trial_balance()
        self.accounts = self.xaf_processor.extract_chart_of_accounts()
        
    def create_income_statement_data(self, year: str) -> pl.DataFrame:
        """Create income statement data from trial balance."""
        if self.trial_balance is None:
            self.load_data()
        
        # Join trial balance with account mapping
        mapped_data = self.trial_balance.join(
            self.account_mapping, 
            left_on='account_id', 
            right_on='account_id', 
            how='inner'
        ).filter(
            pl.col('statement_type') == 'income_statement'
        )
        
        # Calculate net amounts (credit - debit for revenue, debit - credit for expenses)
        income_data = mapped_data.with_columns([
            pl.when(pl.col('account_type').is_in(['Revenue', 'Income']))
            .then(pl.col('turnover_credit') - pl.col('turnover_debit'))
            .otherwise(pl.col('turnover_debit') - pl.col('turnover_credit'))
            .alias('net_amount')
        ]).group_by('Categorie').agg([
            pl.sum('net_amount').alias(f'{year}_total')
        ]).with_columns([
            pl.lit(None).cast(pl.Float64).alias(f'{year}_detail'),
            pl.lit(None).cast(pl.String).alias('Note')
        ])
        
        return income_data
    
    def create_balance_sheet_data(self, year: str) -> pl.DataFrame:
        """Create balance sheet data from trial balance."""
        if self.trial_balance is None:
            self.load_data()
        
        # Join trial balance with account mapping
        mapped_data = self.trial_balance.join(
            self.account_mapping, 
            left_on='account_id', 
            right_on='account_id', 
            how='inner'
        ).filter(
            pl.col('statement_type') == 'balance_sheet'
        )
        
        # Calculate closing balances
        balance_data = mapped_data.with_columns([
            pl.when(pl.col('Balance_Side') == 'Activa')
            .then(pl.col('closing_balance_debit') - pl.col('closing_balance_credit'))
            .otherwise(pl.col('closing_balance_credit') - pl.col('closing_balance_debit'))
            .alias('balance_amount')
        ]).group_by('Categorie').agg([
            pl.sum('balance_amount').alias(f'{year}_total')
        ]).with_columns([
            pl.lit(None).cast(pl.Float64).alias(f'{year}_detail'),
            pl.lit(None).cast(pl.String).alias('Note')
        ])
        
        return balance_data

# ==========================================
# 6. BUSINESS RULES (Enhanced)
# ==========================================

class RuleStatus(Enum):
    PASS = "pass"
    WARNING = "warning" 
    FAIL = "fail"

@dataclass
class BusinessRuleResult:
    rule_name: str
    status: RuleStatus
    message: str
    value: Optional[float] = None
    threshold: Optional[float] = None

class DutchGAAPBusinessRules:
    """Business rules specific to Dutch GAAP reporting."""
    
    @staticmethod
    def validate_balance_sheet_balance(assets: float, liabilities_equity: float, tolerance: float = 0.01) -> BusinessRuleResult:
        """Validate that assets equal liabilities + equity."""
        difference = abs(assets - liabilities_equity)
        
        if difference <= tolerance:
            return BusinessRuleResult(
                rule_name="Balance Sheet Balance", status=RuleStatus.PASS,
                message=f"Balance sheet balances (difference: €{difference:.2f})"
            )
        elif difference <= tolerance * 10:
            return BusinessRuleResult(
                rule_name="Balance Sheet Balance", status=RuleStatus.WARNING,
                message=f"Balance sheet has minor imbalance: €{difference:.2f}",
                value=difference, threshold=tolerance
            )
        else:
            return BusinessRuleResult(
                rule_name="Balance Sheet Balance", status=RuleStatus.FAIL,
                message=f"Balance sheet does not balance: €{difference:.2f}",
                value=difference, threshold=tolerance
            )
    
    @staticmethod
    def validate_revenue_growth(current: float, previous: float, max_decline: float = -20.0) -> BusinessRuleResult:
        """Validate revenue growth is within acceptable limits."""
        if previous == 0:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation", status=RuleStatus.WARNING,
                message="Cannot calculate growth - no previous year revenue"
            )
        
        growth = ((current - previous) / previous) * 100
        
        if growth < max_decline:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation", status=RuleStatus.FAIL,
                message=f"Revenue declined by {growth:.1f}%, exceeds maximum decline of {max_decline}%",
                value=growth, threshold=max_decline
            )
        elif growth < 0:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation", status=RuleStatus.WARNING, 
                message=f"Revenue declined by {growth:.1f}%", value=growth
            )
        else:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation", status=RuleStatus.PASS,
                message=f"Revenue growth of {growth:.1f}% is acceptable", value=growth
            )

# ==========================================
# 7. MAIN COMPREHENSIVE FUNCTION (Updated)
# ==========================================

def create_comprehensive_income_statement(
    raw_data: pl.DataFrame, 
    include_calculations: bool = True, 
    include_validations: bool = True
) -> Tuple[pl.DataFrame, List[CalculationResult], List[BusinessRuleResult]]:
    """Create comprehensive income statement using updated Polars operations."""
    
    # Create mapping
    mapping = create_income_statement_mapping()
    
    # 1. Apply formulas using pivot operations
    result_df, year_columns = calculate_base_subtotals_with_pivot(raw_data, mapping, total_only_categories=[])
    
    # 2. Apply calculations using pivot results
    calculations = []
    if include_calculations:
        calculations = apply_calculations_with_pivot_results(result_df, year_columns)
        
        # Add calculation rows using Polars
        if calculations:
            calc_rows = []
            for calc in calculations:
                calc_row = {
                    'Categorie': calc.name,
                    'Note': f"Formula: {calc.formula}",
                    f"{calc.year}_detail": None,
                    f"{calc.year}_total": calc.value,
                    'row_type': 'calculated',
                    'subtotal_level': 'kpi'
                }
                calc_rows.append(calc_row)
            
            calc_df = pl.DataFrame(calc_rows)
            result_df = pl.concat([result_df, calc_df], how="diagonal")
    
    # 3. Business rules validation
    validations = []
    if include_validations:
        rules = DutchGAAPBusinessRules()
        
        subtotals = result_df.filter(pl.col('row_type') == 'subtotal')
        revenue_rows = subtotals.filter(pl.col('Categorie') == 'Bedrijfsopbrengsten')
        
        if revenue_rows.height > 0 and len(year_columns) >= 2:
            current_year = max(year_columns)
            previous_year = str(int(current_year) - 1)
            
            if f'{current_year}_total' in revenue_rows.columns and f'{previous_year}_total' in revenue_rows.columns:
                rev_current = revenue_rows.select(pl.col(f'{current_year}_total')).item(0, 0)
                rev_previous = revenue_rows.select(pl.col(f'{previous_year}_total')).item(0, 0)
                
                if rev_current and rev_previous:
                    validation = rules.validate_revenue_growth(rev_current, rev_previous)
                    validations.append(validation)
    
    return result_df, calculations, validations

def apply_calculations_with_pivot_results(df: pl.DataFrame, year_columns: List[str]) -> List[CalculationResult]:
    """Apply calculations using pivot results from Polars (updated)."""
    calculations = []
    calc = FinancialCalculations()
    
    # Filter subtotal data for easy access
    subtotal_data = df.filter(pl.col('row_type') == 'subtotal')
    
    for year in year_columns:
        total_col = f"{year}_total"
        
        if total_col in subtotal_data.columns:
            # Extract revenue and costs using Polars filtering
            revenue_row = subtotal_data.filter(pl.col('Categorie') == 'Bedrijfsopbrengsten')
            costs_row = subtotal_data.filter(pl.col('Categorie') == 'Bedrijfskosten')
            
            if revenue_row.height > 0 and costs_row.height > 0:
                revenue = revenue_row.select(pl.col(total_col)).item(0, 0)
                operating_costs = costs_row.select(pl.col(total_col)).item(0, 0)
                
                if revenue and operating_costs:
                    operating_income = revenue - abs(operating_costs)  # Ensure costs are positive for subtraction
                    
                    # Calculate operating margin
                    op_margin = calc.calculate_operating_margin(operating_income, revenue)
                    if op_margin is not None:
                        calculations.append(CalculationResult(
                            name="Operating Margin %", value=op_margin,
                            formula="(Operating Income / Revenue) * 100", year=year
                        ))
                    
                    # Calculate EBITDA (if depreciation data available)
                    depreciation_row = subtotal_data.filter(pl.col('Categorie') == 'Afschrijvingen')
                    if depreciation_row.height > 0:
                        depreciation = depreciation_row.select(pl.col(total_col)).item(0, 0)
                        if depreciation:
                            ebitda = calc.calculate_ebitda(operating_income, abs(depreciation))
                            calculations.append(CalculationResult(
                                name="EBITDA", value=ebitda,
                                formula="Operating Income + Depreciation", year=year
                            ))
                            
                            # EBITDA margin
                            ebitda_margin = calc.calculate_operating_margin(ebitda, revenue)  # Reuse function for margin calculation
                            if ebitda_margin is not None:
                                calculations.append(CalculationResult(
                                    name="EBITDA Margin %", value=ebitda_margin,
                                    formula="(EBITDA / Revenue) * 100", year=year
                                ))
            
            # Financial result calculation
            financial_row = subtotal_data.filter(pl.col('Categorie') == 'Financiële baten en lasten')
            if financial_row.height > 0:
                financial_result = financial_row.select(pl.col(total_col)).item(0, 0)
                if financial_result is not None:
                    calculations.append(CalculationResult(
                        name="Net Financial Result", value=financial_result,
                        formula="Interest Income - Interest Expenses + Other Financial Results", year=year
                    ))
    
    # Revenue growth calculation (only for the most recent year)
    if len(year_columns) >= 2:
        current_year = max(year_columns)
        previous_year = str(int(current_year) - 1)
        
        # Check if previous year data exists in the columns
        if f"{previous_year}_total" in subtotal_data.columns:
            revenue_rows = subtotal_data.filter(pl.col('Categorie') == 'Bedrijfsopbrengsten')
            
            if revenue_rows.height > 0:
                try:
                    revenue_current = revenue_rows.select(pl.col(f'{current_year}_total')).item(0, 0)
                    revenue_previous = revenue_rows.select(pl.col(f'{previous_year}_total')).item(0, 0)
                    
                    if revenue_current is not None and revenue_previous is not None:
                        growth = calc.calculate_revenue_growth(revenue_current, revenue_previous)
                        if growth is not None:
                            calculations.append(CalculationResult(
                                name="Revenue Growth %", value=growth,
                                formula=f"(({current_year} Revenue - {previous_year} Revenue) / {previous_year} Revenue) * 100", 
                                year=current_year
                            ))
                except (IndexError, ValueError) as e:
                    # Handle cases where data extraction fails
                    print(f"Warning: Could not calculate revenue growth: {e}")
    
    # Cost structure analysis
    for year in year_columns:
        total_col = f"{year}_total"
        
        if total_col in subtotal_data.columns:
            revenue_row = subtotal_data.filter(pl.col('Categorie') == 'Bedrijfsopbrengsten')
            personnel_row = subtotal_data.filter(pl.col('Categorie') == 'Personeelskosten')
            
            if revenue_row.height > 0 and personnel_row.height > 0:
                try:
                    revenue = revenue_row.select(pl.col(total_col)).item(0, 0)
                    personnel_costs = personnel_row.select(pl.col(total_col)).item(0, 0)
                    
                    if revenue and personnel_costs and revenue > 0:
                        personnel_ratio = (abs(personnel_costs) / revenue) * 100
                        calculations.append(CalculationResult(
                            name="Personnel Cost Ratio %", value=personnel_ratio,
                            formula="(Personnel Costs / Revenue) * 100", year=year
                        ))
                except (IndexError, ValueError) as e:
                    print(f"Warning: Could not calculate personnel cost ratio for {year}: {e}")
    
    # Return on assets calculation (if balance sheet data is available in the same DataFrame)
    balance_sheet_data = df.filter(pl.col('Balance_Side').is_not_null()) if 'Balance_Side' in df.columns else pl.DataFrame()
    
    if balance_sheet_data.height > 0:
        for year in year_columns:
            total_col = f"{year}_total"
            
            # Get net income (assuming it's calculated as final result)
            income_data = df.filter(pl.col('row_type') == 'original')
            net_income_sources = ['Resultaat na belastingen', 'Net Income', 'Netto resultaat']
            
            net_income = None
            for source in net_income_sources:
                income_row = income_data.filter(pl.col('Categorie') == source)
                if income_row.height > 0 and total_col in income_row.columns:
                    net_income = income_row.select(pl.col(total_col)).item(0, 0)
                    break
            
            # Get total assets
            assets_row = balance_sheet_data.filter(
                (pl.col('Balance_Side') == 'Activa') & 
                (pl.col('row_type') == 'subtotal')
            )
            
            if assets_row.height > 0 and net_income is not None and total_col in assets_row.columns:
                try:
                    total_assets = assets_row.select(pl.col(total_col)).item(0, 0)
                    
                    if total_assets and total_assets > 0:
                        roa = (net_income / total_assets) * 100
                        calculations.append(CalculationResult(
                            name="Return on Assets %", value=roa,
                            formula="(Net Income / Total Assets) * 100", year=year
                        ))
                except (IndexError, ValueError) as e:
                    print(f"Warning: Could not calculate ROA for {year}: {e}")
    
    # Working capital calculation (if balance sheet data available)
    if balance_sheet_data.height > 0:
        for year in year_columns:
            total_col = f"{year}_total"
            
            try:
                # Current assets (simplified - using vlottende activa)
                current_assets_row = balance_sheet_data.filter(
                    (pl.col('Level0') == 'Vlottende activa') & 
                    (pl.col('row_type') == 'subtotal')
                )
                
                # Current liabilities 
                current_liabilities_row = balance_sheet_data.filter(
                    pl.col('Categorie') == 'Kortlopende schulden'
                )
                
                if (current_assets_row.height > 0 and current_liabilities_row.height > 0 and 
                    total_col in current_assets_row.columns and total_col in current_liabilities_row.columns):
                    
                    current_assets = current_assets_row.select(pl.col(total_col)).item(0, 0)
                    current_liabilities = current_liabilities_row.select(pl.col(total_col)).item(0, 0)
                    
                    if current_assets is not None and current_liabilities is not None:
                        working_capital = current_assets - current_liabilities
                        calculations.append(CalculationResult(
                            name="Working Capital", value=working_capital,
                            formula="Current Assets - Current Liabilities", year=year
                        ))
                        
                        # Current ratio
                        if current_liabilities != 0:
                            current_ratio = calc.calculate_current_ratio(current_assets, current_liabilities)
                            if current_ratio is not None:
                                calculations.append(CalculationResult(
                                    name="Current Ratio", value=current_ratio,
                                    formula="Current Assets / Current Liabilities", year=year
                                ))
            except (IndexError, ValueError) as e:
                print(f"Warning: Could not calculate working capital for {year}: {e}")
    
    return calculations


# ==========================================
# 8. QUARTO REPORT GENERATOR (FIXED)
# ==========================================

import datetime

class QuartoFinancialReport:
    """Generate Quarto-compatible financial reports."""
    
    def __init__(self, company_name: str, report_year: str):
        self.company_name = company_name
        self.report_year = report_year
        self.financial_statements = {}
        self.metadata = {
            'generation_date': None,
            'data_source': None,
            'validation_results': []
        }
    
    def add_financial_statement(self, statement_name: str, data: pl.DataFrame, calculations: List[CalculationResult] = None):
        """Add a financial statement to the report."""
        self.financial_statements[statement_name] = {
            'data': data,
            'calculations': calculations or [],
            'added_date': datetime.datetime.now()  # Fixed: use Python datetime instead of pl.datetime
        }
    
    def add_validation_results(self, validations: List[BusinessRuleResult]):
        """Add business rule validation results."""
        self.metadata['validation_results'].extend(validations)
    
    def generate_report(self, output_dir: Path) -> Path:
        """Generate Quarto markdown file."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save all financial statement data as CSV files
        for statement_name, statement_data in self.financial_statements.items():
            if 'data' in statement_data and isinstance(statement_data['data'], pl.DataFrame):
                # Clean statement name for filename
                clean_name = statement_name.lower().replace(' ', '_').replace('-', '_')
                csv_file = output_dir / f"{clean_name}_data.csv"
                statement_data['data'].write_csv(csv_file)
                
                # Save calculations if available
                if statement_data['calculations']:
                    calc_data = []
                    for calc in statement_data['calculations']:
                        calc_data.append({
                            'name': calc.name,
                            'value': calc.value,
                            'formula': calc.formula,
                            'year': calc.year,
                            'category': calc.category
                        })
                    calc_df = pl.DataFrame(calc_data)
                    calc_file = output_dir / f"{clean_name}_calculations.csv"
                    calc_df.write_csv(calc_file)
        
        # Generate basic Quarto file
        qmd_content = self._generate_basic_qmd()
        qmd_file = output_dir / f"financial_report_{self.report_year}.qmd"
        
        with open(qmd_file, 'w', encoding='utf-8') as f:
            f.write(qmd_content)
        
        return qmd_file
    
    def _generate_basic_qmd(self) -> str:
        """Generate basic Quarto markdown content."""
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fixed datetime usage
        
        return f'''---
title: "Financial Report {self.report_year}"
subtitle: "{self.company_name}"
date: today
format:
  html:
    toc: true
    theme: cosmo
  pdf:
    toc: true
execute:
  echo: false
  warning: false
---

# {self.company_name} - Financial Report {self.report_year}

This is an automated financial report generated from processed financial data.

## Financial Statements

```{{python}}
import polars as pl
import pandas as pd
from pathlib import Path

# Load available financial statement data
data_files = list(Path(".").glob("*_data.csv"))
for file in data_files:
    statement_name = file.stem.replace("_data", "").replace("_", " ").title()
    print(f"\\n### {{statement_name}}")
    
    try:
        df = pl.read_csv(file)
        print(f"Data loaded: {{df.shape[0]}} rows, {{df.shape[1]}} columns")
        
        # Display first few rows
        display_df = df.head(10).to_pandas()
        print(display_df.to_string(index=False))
        
    except Exception as e:
        print(f"Error loading {{file}}: {{e}}")
```

## Calculations and KPIs

```{{python}}
# Load calculation files if available
calc_files = list(Path(".").glob("*_calculations.csv"))
for file in calc_files:
    statement_name = file.stem.replace("_calculations", "").replace("_", " ").title()
    print(f"\\n### {{statement_name}} - Key Performance Indicators")
    
    try:
        calc_df = pl.read_csv(file)
        calc_pandas = calc_df.to_pandas()
        print(calc_pandas.to_string(index=False))
        
    except Exception as e:
        print(f"Error loading calculations from {{file}}: {{e}}")
```

---
*Report generated automatically on {current_time}*
'''
    
    
# ==========================================
# 9. AUTOMATED REPORT PIPELINE
# ==========================================

class AutomatedReportPipeline:
    """Complete pipeline for automated financial reporting from XAF to Quarto."""
    
    def __init__(self, company_name: str, report_year: str):
        self.company_name = company_name
        self.report_year = report_year
        self.previous_year = str(int(report_year) - 1)
        
        # Core components
        self.xaf_processor: Optional[XAFProcessor] = None
        self.xaf_converter: Optional[XAFToFinancialStatements] = None
        self.account_mapping: Optional[pl.DataFrame] = None
        self.quarto_report = QuartoFinancialReport(company_name, report_year)
        
        # Data storage
        self.financial_data = {}
        self.validation_results = []
        
    def load_xaf_file(self, xaf_file_path: str) -> bool:
        """Load XAF file and initialize processor."""
        try:
            self.xaf_processor = XAFProcessor(xaf_file_path)
            return self.xaf_processor.load_xaf_file()
        except Exception as e:
            print(f"Error loading XAF file: {e}")
            return False
    
    def set_account_mapping(self, mapping: pl.DataFrame):
        """Set the account mapping for financial statement conversion."""
        self.account_mapping = mapping
        if self.xaf_processor:
            self.xaf_converter = XAFToFinancialStatements(self.xaf_processor, mapping)
    
    def process_financial_statements(self) -> Dict[str, Any]:
        """Process financial statements from XAF data."""
        if not self.xaf_converter:
            raise ValueError("XAF converter not initialized. Set account mapping first.")
        
        results = {}
        
        try:
            # Process Income Statement
            income_data = self.xaf_converter.create_income_statement_data(self.report_year)
            
            # Add previous year data if available (simplified - would need previous year XAF)
            income_data = income_data.with_columns([
                pl.lit(0).alias(f'{self.previous_year}_total'),
                pl.lit(None).cast(pl.Float64).alias(f'{self.previous_year}_detail')
            ])
            
            # Process with comprehensive calculations
            income_result, income_calcs, income_validations = create_comprehensive_income_statement(
                income_data, include_calculations=True, include_validations=True
            )
            
            # Add to Quarto report
            self.quarto_report.add_financial_statement("Income Statement", income_result, income_calcs)
            self.validation_results.extend(income_validations)
            results['income_statement'] = {
                'data': income_result,
                'calculations': income_calcs,
                'validations': income_validations
            }
            
            # Process Balance Sheet
            balance_data = self.xaf_converter.create_balance_sheet_data(self.report_year)
            
            # Add previous year data
            balance_data = balance_data.with_columns([
                pl.lit(0).alias(f'{self.previous_year}_total'),
                pl.lit(None).cast(pl.Float64).alias(f'{self.previous_year}_detail')
            ])
            
            # Apply balance sheet mapping
            balance_mapping = create_balance_sheet_mapping()
            balance_result, balance_years = calculate_base_subtotals_with_pivot(
                balance_data, balance_mapping, total_only_categories=[]
            )
            
            # Add to Quarto report
            self.quarto_report.add_financial_statement("Balance Sheet", balance_result)
            results['balance_sheet'] = {
                'data': balance_result,
                'years': balance_years
            }
            
            # Validate balance sheet
            if balance_result.height > 0:
                # Extract totals for validation
                assets_row = balance_result.filter(
                    (pl.col('Balance_Side') == 'Activa') & 
                    (pl.col('row_type') == 'subtotal')
                )
                passiva_row = balance_result.filter(
                    (pl.col('Balance_Side') == 'Passiva') & 
                    (pl.col('row_type') == 'subtotal')
                )
                
                if assets_row.height > 0 and passiva_row.height > 0:
                    assets_total = assets_row.select(pl.col(f'{self.report_year}_total')).item(0, 0)
                    passiva_total = passiva_row.select(pl.col(f'{self.report_year}_total')).item(0, 0)
                    
                    if assets_total and passiva_total:
                        rules = DutchGAAPBusinessRules()
                        balance_validation = rules.validate_balance_sheet_balance(assets_total, passiva_total)
                        self.validation_results.append(balance_validation)
                        results['balance_validation'] = balance_validation
            
            # Add all validations to Quarto report
            self.quarto_report.add_validation_results(self.validation_results)
            
            return results
            
        except Exception as e:
            print(f"Error processing financial statements: {e}")
            raise
    
    def generate_report(self, output_dir: Path) -> Path:
        """Generate final Quarto report."""
        return self.quarto_report.generate_report(output_dir)
    
    def get_validation_summary(self) -> Dict[str, int]:
        """Get summary of validation results."""
        summary = {'pass': 0, 'warning': 0, 'fail': 0}
        for validation in self.validation_results:
            summary[validation.status.value] += 1
        return summary