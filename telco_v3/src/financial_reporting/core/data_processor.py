"""
Data Processing Module

Handles financial data transformation, pivot operations, and statement generation.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl

from financial_reporting.core.calculations import (
    CalculationResult,
    DutchGAAPBusinessRules,
    FinancialCalculations,
    apply_calculations_with_pivot_results,
)
from financial_reporting.core.xaf_processor import XAFProcessor

try:
    from version import __version__
except ImportError:
    __version__ = "0.3.0"


# ---------------------------------------------------------------------------
# Hierarchy mapping tables
# ---------------------------------------------------------------------------


def create_income_statement_mapping() -> pl.DataFrame:
    """Define the hierarchical structure for income statement (Dutch GAAP)."""
    return pl.DataFrame(
        {
            "Categorie": [
                "Netto-omzet",
                "Overige bedrijfsopbrengsten",
                "Personeelskosten",
                "Afschrijvingen",
                "Overige bedrijfskosten",
                "Rentebaten",
                "Rentelasten",
                "Waardeveranderingen effecten",
                "Vennootschapsbelasting",
                "Resultaat deelnemingen",
            ],
            "Level0": [
                "Bedrijfsopbrengsten",
                "Bedrijfsopbrengsten",
                "Bedrijfskosten",
                "Bedrijfskosten",
                "Bedrijfskosten",
                "Financiële baten en lasten",
                "Financiële baten en lasten",
                "Financiële baten en lasten",
                "Belastingen en deelnemingen",
                "Belastingen en deelnemingen",
            ],
            "Level1": [
                "Omzet",
                "Omzet",
                "Personeelskosten",
                "Afschrijvingen",
                "Overige kosten",
                "Rentebaten",
                "Rentelasten",
                "Overige financiële resultaten",
                "Belastingen",
                "Resultaat deelnemingen",
            ],
            "Sort1": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
            "Display_Order": list(range(1, 11)),
            "In_Subtotal": [True] * 10,
        }
    )


def create_balance_sheet_mapping() -> pl.DataFrame:
    """Define the hierarchical structure for balance sheet (Dutch GAAP)."""
    return pl.DataFrame(
        {
            "Categorie": [
                "Materiële vaste activa",
                "Financiële vaste activa",
                "Vastgoedbeleggingen",
                "Vorderingen",
                "Effecten",
                "Liquide middelen",
                "Eigen vermogen",
                "Voorzieningen",
                "Langlopende schulden",
                "Kortlopende schulden",
            ],
            "Level0": [
                "Vaste activa",
                "Vaste activa",
                "Vaste activa",
                "Vlottende activa",
                "Vlottende activa",
                "Vlottende activa",
                "Eigen vermogen",
                "Voorzieningen",
                "Vreemd vermogen langlopend",
                "Vreemd vermogen kortlopend",
            ],
            "Level1": [
                "Materiële vaste activa",
                "Financiële vaste activa",
                "Vastgoedbeleggingen",
                "Vorderingen",
                "Effecten",
                "Liquide middelen",
                "Eigen vermogen",
                "Voorzieningen",
                "Langlopende schulden",
                "Kortlopende schulden",
            ],
            "Sort1": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
            "Display_Order": list(range(1, 11)),
            "In_Subtotal": [True] * 10,
            "Balance_Side": ["Activa"] * 6 + ["Passiva"] * 4,
        }
    )


# ---------------------------------------------------------------------------
# Column name cleanup
# ---------------------------------------------------------------------------


def fix_pivot_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Fix problematic column names created by Polars pivot operations.

    Polars pivot can create complex JSON-like column names.  This function
    cleans them up to produce readable names like '2024_total' / '2024_detail'.
    """
    new_columns: Dict[str, str] = {}

    for col in df.columns:
        if col == "Categorie" or not isinstance(col, str):
            continue
        if "{" in col and "}" in col:
            year_match = re.search(r'"(\d{4})"', col)
            if year_match:
                year = year_match.group(1)
                if '"total"' in col.lower():
                    new_columns[col] = f"{year}_total"
                elif '"detail"' in col.lower():
                    new_columns[col] = f"{year}_detail"
                else:
                    new_columns[col] = year

    if new_columns:
        df = df.rename(new_columns)

    return df


# ---------------------------------------------------------------------------
# Pivot-based aggregation helpers
# ---------------------------------------------------------------------------


def prepare_data_for_pivot(
    raw_data: pl.DataFrame,
    mapping: pl.DataFrame,
    total_only_categories: Optional[List[str]] = None,
) -> pl.DataFrame:
    """Prepare data in long format for pivot operations using Polars."""
    if total_only_categories is None:
        total_only_categories = []

    combined = raw_data.join(mapping, on="Categorie", how="left").filter(
        pl.col("Level0").is_not_null()
    )

    excluded_cols = [
        "Categorie",
        "Note",
        "Level0",
        "Level1",
        "Sort1",
        "Display_Order",
        "In_Subtotal",
    ]
    if "Balance_Side" in combined.columns:
        excluded_cols.append("Balance_Side")

    year_columns = [col for col in combined.columns if col not in excluded_cols]

    detail_cols = [col for col in year_columns if col.endswith("_detail")]
    total_cols = [col for col in year_columns if col.endswith("_total")]

    base_columns = ["Categorie", "Level0", "Level1", "Sort1"]
    if "Balance_Side" in combined.columns:
        base_columns.append("Balance_Side")

    if detail_cols and total_cols:
        detail_melted = (
            combined.select(base_columns + detail_cols)
            .unpivot(
                index=base_columns,
                on=detail_cols,
                variable_name="Year_Type",
                value_name="Amount",
            )
            .with_columns(
                [
                    pl.col("Year_Type").str.replace("_detail", "").alias("Year"),
                    pl.lit("detail").alias("Type"),
                    pl.col("Amount").cast(pl.Float64, strict=False),
                ]
            )
            .drop("Year_Type")
        )

        total_melted = (
            combined.select(base_columns + total_cols)
            .unpivot(
                index=base_columns,
                on=total_cols,
                variable_name="Year_Type",
                value_name="Amount",
            )
            .with_columns(
                [
                    pl.col("Year_Type").str.replace("_total", "").alias("Year"),
                    pl.lit("total").alias("Type"),
                    pl.col("Amount").cast(pl.Float64, strict=False),
                ]
            )
            .drop("Year_Type")
        )

        all_columns = set(detail_melted.columns) | set(total_melted.columns)
        for col in all_columns:
            if col not in detail_melted.columns:
                detail_melted = detail_melted.with_columns(
                    pl.lit(None).cast(pl.String).alias(col)
                )
            if col not in total_melted.columns:
                total_melted = total_melted.with_columns(
                    pl.lit(None).cast(pl.String).alias(col)
                )

        column_order = sorted(all_columns)
        detail_melted = detail_melted.select(column_order)
        total_melted = total_melted.select(column_order)

        long_data = pl.concat([detail_melted, total_melted], how="vertical")

        long_data = long_data.with_columns(
            [
                pl.when(
                    pl.col("Categorie").is_in(total_only_categories)
                    & (pl.col("Type") == "detail")
                    & pl.col("Amount").is_not_null()
                )
                .then(pl.lit("total"))
                .otherwise(pl.col("Type"))
                .alias("Type"),
                pl.when(
                    pl.col("Categorie").is_in(total_only_categories)
                    & (pl.col("Type") == "detail")
                )
                .then(pl.lit(None).cast(pl.Float64))
                .otherwise(pl.col("Amount"))
                .alias("Amount"),
            ]
        )

    else:
        long_data = (
            combined.select(base_columns + year_columns)
            .unpivot(
                index=base_columns,
                on=year_columns,
                variable_name="Year",
                value_name="Amount",
            )
            .with_columns(
                [
                    pl.when(pl.col("Categorie").is_in(total_only_categories))
                    .then(pl.lit("total"))
                    .otherwise(pl.lit("detail"))
                    .alias("Type"),
                    pl.col("Amount").cast(pl.Float64, strict=False),
                ]
            )
        )

    return long_data.filter(pl.col("Amount").is_not_null())


def calculate_subtotals_with_pivot(
    long_data: pl.DataFrame,
    total_only_categories: Optional[List[str]] = None,
) -> List[pl.DataFrame]:
    """Calculate hierarchical subtotals using Polars group_by."""
    if total_only_categories is None:
        total_only_categories = []

    detail_data = long_data.filter(
        ~pl.col("Categorie").is_in(total_only_categories) & (pl.col("Type") == "detail")
    )

    subtotal_frames: List[pl.DataFrame] = []

    if detail_data.height == 0:
        return subtotal_frames

    level0_agg = (
        detail_data.group_by(["Level0", "Year"])
        .agg([pl.sum("Amount").alias("Amount")])
        .with_columns(
            [
                pl.col("Level0").alias("Categorie"),
                pl.lit(None).cast(pl.String).alias("Level1"),
                pl.lit(None).cast(pl.String).alias("Sort1"),
                pl.lit("total").alias("Type"),
                pl.lit("subtotal").alias("row_type"),
                pl.lit("Level0").alias("subtotal_level"),
            ]
        )
    )

    if "Balance_Side" in detail_data.columns:
        bs_mapping = detail_data.group_by("Level0").agg(
            [pl.first("Balance_Side").alias("Balance_Side")]
        )
        level0_agg = level0_agg.join(bs_mapping, on="Level0", how="left")
    else:
        level0_agg = level0_agg.with_columns(pl.lit(None).cast(pl.String).alias("Balance_Side"))

    subtotal_frames.append(level0_agg)

    level1_detail = detail_data.filter(pl.col("Level1").is_not_null())
    if level1_detail.height > 0:
        level1_agg = (
            level1_detail.group_by(["Level0", "Level1", "Year"])
            .agg([pl.sum("Amount").alias("Amount")])
            .with_columns(
                [
                    pl.col("Level1").alias("Categorie"),
                    pl.lit(None).cast(pl.String).alias("Sort1"),
                    pl.lit("total").alias("Type"),
                    pl.lit("subtotal").alias("row_type"),
                    pl.lit("Level1").alias("subtotal_level"),
                ]
            )
        )

        if "Balance_Side" in detail_data.columns:
            bs_mapping = level1_detail.group_by(["Level0", "Level1"]).agg(
                [pl.first("Balance_Side").alias("Balance_Side")]
            )
            level1_agg = level1_agg.join(bs_mapping, on=["Level0", "Level1"], how="left")
        else:
            level1_agg = level1_agg.with_columns(
                pl.lit(None).cast(pl.String).alias("Balance_Side")
            )

        subtotal_frames.append(level1_agg)

    return subtotal_frames


def calculate_base_subtotals_with_pivot(
    raw_data: pl.DataFrame,
    mapping: pl.DataFrame,
    total_only_categories: Optional[List[str]] = None,
) -> Tuple[pl.DataFrame, List[str]]:
    """Compute pivot table with hierarchical subtotals."""
    long_data = prepare_data_for_pivot(raw_data, mapping, total_only_categories)
    long_data = long_data.with_columns(
        [
            pl.lit("original").alias("row_type"),
            pl.lit(None).cast(pl.String).alias("subtotal_level"),
        ]
    )

    subtotal_frames = calculate_subtotals_with_pivot(long_data, total_only_categories)

    all_frames = [long_data] + subtotal_frames
    if len(all_frames) > 1:
        all_columns: set = set()
        for frame in all_frames:
            all_columns.update(frame.columns)

        aligned_frames = []
        for frame in all_frames:
            aligned_frame = frame
            for col in all_columns:
                if col not in frame.columns:
                    if col == "Amount":
                        aligned_frame = aligned_frame.with_columns(
                            pl.lit(None).cast(pl.Float64).alias(col)
                        )
                    else:
                        aligned_frame = aligned_frame.with_columns(
                            pl.lit(None).cast(pl.String).alias(col)
                        )
            column_order = sorted(all_columns)
            aligned_frame = aligned_frame.select(column_order)
            aligned_frames.append(aligned_frame)

        combined_long = pl.concat(aligned_frames, how="vertical")
    else:
        combined_long = long_data

    index_cols = ["Categorie", "Level0", "Level1", "Sort1", "row_type", "subtotal_level"]
    if "Balance_Side" in combined_long.columns:
        index_cols.append("Balance_Side")

    try:
        wide_result = combined_long.pivot(
            values="Amount",
            index=index_cols,
            on=["Year", "Type"],
            aggregate_function="first",
        )
    except Exception as e:
        print(f"Pivot operation failed: {e}")
        print("Available columns:", combined_long.columns)
        raise

    year_columns: List[str] = []
    rename_map: Dict[Any, str] = {}

    for col in wide_result.columns:
        if isinstance(col, tuple) and len(col) == 2:
            year, type_suffix = col
            new_name = f"{year}_{type_suffix}" if type_suffix else str(year)
            rename_map[col] = new_name
            if year not in year_columns:
                year_columns.append(year)

    if rename_map:
        wide_result = wide_result.rename(rename_map)

    # Clean up any remaining JSON-like column names
    wide_result = fix_pivot_column_names(wide_result)

    if "Note" not in wide_result.columns:
        wide_result = wide_result.with_columns([pl.lit(None).cast(pl.String).alias("Note")])

    sort_columns = [
        c for c in ["Level0", "Level1", "Sort1", "row_type", "Categorie"] if c in wide_result.columns
    ]
    if sort_columns:
        wide_result = wide_result.sort(sort_columns)

    return wide_result, year_columns


def create_comprehensive_income_statement(
    raw_data: pl.DataFrame,
    include_calculations: bool = True,
    include_validations: bool = True,
) -> Tuple[pl.DataFrame, List[CalculationResult], list]:
    """Create comprehensive income statement using Polars pivot operations."""
    from financial_reporting.core.calculations import BusinessRuleResult

    mapping = create_income_statement_mapping()
    result_df, year_columns = calculate_base_subtotals_with_pivot(raw_data, mapping, [])

    calculations: List[CalculationResult] = []
    if include_calculations:
        calculations = apply_calculations_with_pivot_results(result_df, year_columns)
        if calculations:
            calc_rows = []
            for calc in calculations:
                calc_row: Dict[str, Any] = {
                    "Categorie": calc.name,
                    "Note": f"Formula: {calc.formula}",
                    f"{calc.year}_detail": None,
                    f"{calc.year}_total": calc.value,
                    "row_type": "calculated",
                    "subtotal_level": "kpi",
                }
                calc_rows.append(calc_row)
            calc_df = pl.DataFrame(calc_rows)
            result_df = pl.concat([result_df, calc_df], how="diagonal")

    validations: List[BusinessRuleResult] = []
    if include_validations:
        rules = DutchGAAPBusinessRules()
        subtotals = result_df.filter(pl.col("row_type") == "subtotal")
        revenue_rows = subtotals.filter(pl.col("Categorie") == "Bedrijfsopbrengsten")

        if revenue_rows.height > 0 and len(year_columns) >= 2:
            current_year = max(year_columns)
            previous_year = str(int(current_year) - 1)
            cy_col = f"{current_year}_total"
            py_col = f"{previous_year}_total"

            if cy_col in revenue_rows.columns and py_col in revenue_rows.columns:
                rev_current = revenue_rows.select(pl.col(cy_col)).item(0, 0)
                rev_previous = revenue_rows.select(pl.col(py_col)).item(0, 0)
                if rev_current and rev_previous:
                    validation = rules.validate_revenue_growth(rev_current, rev_previous)
                    validations.append(validation)

    return result_df, calculations, validations


# ---------------------------------------------------------------------------
# XAF → financial statements converter
# ---------------------------------------------------------------------------


class XAFToFinancialStatements:
    """Convert XAF data to standardised financial statements."""

    def __init__(self, xaf_processor: XAFProcessor, account_mapping: pl.DataFrame) -> None:
        self.xaf_processor = xaf_processor
        self.account_mapping = account_mapping
        self.trial_balance: Optional[pl.DataFrame] = None
        self.accounts: Optional[pl.DataFrame] = None

    def load_data(self) -> None:
        if not self.xaf_processor.load_xaf_file():
            raise ValueError("Failed to load XAF file")
        self.trial_balance = self.xaf_processor.extract_trial_balance()
        self.accounts = self.xaf_processor.extract_chart_of_accounts()

    def create_income_statement_data(self, year: str) -> pl.DataFrame:
        if self.trial_balance is None:
            self.load_data()
        mapped_data = self.trial_balance.join(  # type: ignore[union-attr]
            self.account_mapping,
            left_on="account_id",
            right_on="account_id",
            how="inner",
        ).filter(pl.col("statement_type") == "income_statement")

        return (
            mapped_data.with_columns(
                [
                    pl.when(pl.col("account_type").is_in(["Revenue", "Income"]))
                    .then(pl.col("turnover_credit") - pl.col("turnover_debit"))
                    .otherwise(pl.col("turnover_debit") - pl.col("turnover_credit"))
                    .alias("net_amount")
                ]
            )
            .group_by("Categorie")
            .agg([pl.sum("net_amount").alias(f"{year}_total")])
            .with_columns(
                [
                    pl.lit(None).cast(pl.Float64).alias(f"{year}_detail"),
                    pl.lit(None).cast(pl.String).alias("Note"),
                ]
            )
        )

    def create_balance_sheet_data(self, year: str) -> pl.DataFrame:
        if self.trial_balance is None:
            self.load_data()
        mapped_data = self.trial_balance.join(  # type: ignore[union-attr]
            self.account_mapping,
            left_on="account_id",
            right_on="account_id",
            how="inner",
        ).filter(pl.col("statement_type") == "balance_sheet")

        return (
            mapped_data.with_columns(
                [
                    pl.when(pl.col("Balance_Side") == "Activa")
                    .then(pl.col("closing_balance_debit") - pl.col("closing_balance_credit"))
                    .otherwise(pl.col("closing_balance_credit") - pl.col("closing_balance_debit"))
                    .alias("balance_amount")
                ]
            )
            .group_by("Categorie")
            .agg([pl.sum("balance_amount").alias(f"{year}_total")])
            .with_columns(
                [
                    pl.lit(None).cast(pl.Float64).alias(f"{year}_detail"),
                    pl.lit(None).cast(pl.String).alias("Note"),
                ]
            )
        )


# ---------------------------------------------------------------------------
# Report data processor
# ---------------------------------------------------------------------------


class ReportDataProcessor:
    """Handles all data processing logic separately from Quarto templates."""

    def __init__(self, company_name: str, report_year: str) -> None:
        self.company_name = company_name
        self.report_year = report_year
        self.data: Dict[str, Any] = {}
        self.calculations: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}

    def process_income_statement(self, income_df: pl.DataFrame) -> Dict[str, Any]:
        income_df = self._fix_column_names(income_df)
        income_pd = income_df.to_pandas()
        current_year = self.report_year
        previous_year = str(int(current_year) - 1)

        revenue_current = self._safe_get_value(income_pd, "Netto-omzet", current_year)
        revenue_previous = self._safe_get_value(income_pd, "Netto-omzet", previous_year)

        return {
            "dataframe": income_pd,
            "revenue_current": revenue_current,
            "revenue_previous": revenue_previous,
            "revenue_change": revenue_current - revenue_previous if revenue_previous else None,
            "revenue_change_pct": (
                (revenue_current - revenue_previous) / revenue_previous * 100
                if revenue_previous and revenue_previous != 0
                else None
            ),
        }

    def process_balance_sheet(self, balance_df: pl.DataFrame) -> Dict[str, Any]:
        balance_df = self._fix_column_names(balance_df)
        balance_pd = balance_df.to_pandas()
        current_year = self.report_year
        previous_year = str(int(current_year) - 1)

        return {
            "dataframe": balance_pd,
            "total_assets_current": self._calculate_total_assets(balance_pd, current_year),
            "total_assets_previous": self._calculate_total_assets(balance_pd, previous_year),
            "working_capital": self._calculate_working_capital(balance_pd, current_year),
        }

    def generate_chart_data(self, data_type: str) -> Dict[str, Any]:
        if data_type == "revenue_trend":
            return {
                "years": [self.report_year, str(int(self.report_year) - 1)],
                "values": [
                    self.data.get("income_statement", {}).get("revenue_current", 0),
                    self.data.get("income_statement", {}).get("revenue_previous", 0),
                ],
            }
        elif data_type == "assets_composition":
            balance_data = self.data.get("balance_sheet", {}).get("dataframe")
            if balance_data is not None:
                return self._prepare_assets_chart_data(balance_data)
        return {}

    def save_processed_data(self, output_dir: Path) -> None:
        data_dir = output_dir / "processed_data"
        data_dir.mkdir(exist_ok=True)

        if "income_statement" in self.data:
            self.data["income_statement"]["dataframe"].to_csv(
                data_dir / "income_statement.csv", index=False
            )
        if "balance_sheet" in self.data:
            self.data["balance_sheet"]["dataframe"].to_csv(
                data_dir / "balance_sheet.csv", index=False
            )

        with open(data_dir / "calculations.json", "w") as f:
            json.dump(self.calculations, f, indent=2, default=str)

        with open(data_dir / "metadata.json", "w") as f:
            json.dump(
                {
                    "company_name": self.company_name,
                    "report_year": self.report_year,
                    "generation_date": datetime.now().isoformat(),
                    "system_version": __version__,
                    **self.metadata,
                },
                f,
                indent=2,
                default=str,
            )

        chart_data = {
            "revenue_trend": self.generate_chart_data("revenue_trend"),
            "assets_composition": self.generate_chart_data("assets_composition"),
        }
        with open(data_dir / "chart_data.json", "w") as f:
            json.dump(chart_data, f, indent=2, default=str)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fix_column_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fix problematic column names from Polars pivot operations."""
        current_columns = df.columns
        rename_mapping: Dict[str, str] = {}

        for col in current_columns:
            if col in [
                "Categorie",
                "Level0",
                "Level1",
                "Sort1",
                "row_type",
                "subtotal_level",
                "Balance_Side",
                "Note",
            ]:
                continue
            if '{"' in col and '"}' in col:
                cleaned = col.strip("{}")
                parts = []
                for part in cleaned.split(","):
                    clean_part = re.sub(r'["\s\\]', "", part)
                    if clean_part:
                        parts.append(clean_part)
                if parts:
                    rename_mapping[col] = "_".join(parts)

        if rename_mapping:
            df = df.rename(rename_mapping)
        return df

    def _safe_get_value(
        self, df: pd.DataFrame, category: str, year_col: str
    ) -> float:
        try:
            mask = df["Categorie"] == category
            if mask.any() and year_col in df.columns:
                return float(df.loc[mask, year_col].iloc[0])
        except (IndexError, ValueError, KeyError):
            pass
        return 0.0

    def _calculate_total_assets(self, df: pd.DataFrame, year_col: str) -> float:
        asset_categories = [
            "Vastgoedbeleggingen",
            "Financiële vaste activa",
            "Vorderingen",
            "Liquide middelen",
        ]
        return sum(self._safe_get_value(df, cat, year_col) for cat in asset_categories)

    def _calculate_working_capital(self, df: pd.DataFrame, year_col: str) -> float:
        current_assets = self._safe_get_value(
            df, "Vorderingen", year_col
        ) + self._safe_get_value(df, "Liquide middelen", year_col)
        current_liabilities = self._safe_get_value(df, "Kortlopende schulden", year_col)
        return current_assets - current_liabilities

    def _prepare_assets_chart_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        current_year = self.report_year
        categories = [
            "Vastgoedbeleggingen",
            "Financiële vaste activa",
            "Vorderingen",
            "Liquide middelen",
        ]
        return {
            "categories": categories,
            "values": [self._safe_get_value(df, cat, current_year) for cat in categories],
        }


__all__ = [
    "create_income_statement_mapping",
    "create_balance_sheet_mapping",
    "fix_pivot_column_names",
    "prepare_data_for_pivot",
    "calculate_subtotals_with_pivot",
    "calculate_base_subtotals_with_pivot",
    "create_comprehensive_income_statement",
    "XAFToFinancialStatements",
    "ReportDataProcessor",
]
