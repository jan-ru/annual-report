"""
Financial Calculations Module

Handles financial calculations, ratios, and Dutch GAAP business rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import polars as pl


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
    def validate_balance_sheet_balance(
        assets: float, liabilities_equity: float, tolerance: float = 0.01
    ) -> BusinessRuleResult:
        """Validate that assets equal liabilities + equity."""
        difference = abs(assets - liabilities_equity)
        if difference <= tolerance:
            return BusinessRuleResult(
                rule_name="Balance Sheet Balance",
                status=RuleStatus.PASS,
                message=f"Balance sheet balances (difference: €{difference:.2f})",
            )
        elif difference <= tolerance * 10:
            return BusinessRuleResult(
                rule_name="Balance Sheet Balance",
                status=RuleStatus.WARNING,
                message=f"Balance sheet has minor imbalance: €{difference:.2f}",
                value=difference,
                threshold=tolerance,
            )
        else:
            return BusinessRuleResult(
                rule_name="Balance Sheet Balance",
                status=RuleStatus.FAIL,
                message=f"Balance sheet does not balance: €{difference:.2f}",
                value=difference,
                threshold=tolerance,
            )

    @staticmethod
    def validate_revenue_growth(
        current: float, previous: float, max_decline: float = -20.0
    ) -> BusinessRuleResult:
        """Validate revenue growth is within acceptable limits."""
        if previous == 0:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation",
                status=RuleStatus.WARNING,
                message="Cannot calculate growth - no previous year revenue",
            )
        growth = ((current - previous) / previous) * 100
        if growth < max_decline:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation",
                status=RuleStatus.FAIL,
                message=f"Revenue declined by {growth:.1f}%, exceeds maximum decline of {max_decline}%",
                value=growth,
                threshold=max_decline,
            )
        elif growth < 0:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation",
                status=RuleStatus.WARNING,
                message=f"Revenue declined by {growth:.1f}%",
                value=growth,
            )
        else:
            return BusinessRuleResult(
                rule_name="Revenue Growth Validation",
                status=RuleStatus.PASS,
                message=f"Revenue growth of {growth:.1f}% is acceptable",
                value=growth,
            )


def apply_calculations_with_pivot_results(
    df: pl.DataFrame, year_columns: List[str]
) -> List[CalculationResult]:
    """Apply financial calculations using Polars pivot results."""
    calculations: List[CalculationResult] = []
    calc = FinancialCalculations()

    subtotal_data = df.filter(pl.col("row_type") == "subtotal")

    for year in year_columns:
        total_col = f"{year}_total"

        if total_col not in subtotal_data.columns:
            continue

        revenue_row = subtotal_data.filter(pl.col("Categorie") == "Bedrijfsopbrengsten")
        costs_row = subtotal_data.filter(pl.col("Categorie") == "Bedrijfskosten")

        if revenue_row.height > 0 and costs_row.height > 0:
            revenue = revenue_row.select(pl.col(total_col)).item(0, 0)
            operating_costs = costs_row.select(pl.col(total_col)).item(0, 0)

            if revenue and operating_costs:
                operating_income = revenue - abs(operating_costs)

                op_margin = calc.calculate_operating_margin(operating_income, revenue)
                if op_margin is not None:
                    calculations.append(
                        CalculationResult(
                            name="Operating Margin %",
                            value=op_margin,
                            formula="(Operating Income / Revenue) * 100",
                            year=year,
                        )
                    )

                depreciation_row = subtotal_data.filter(pl.col("Categorie") == "Afschrijvingen")
                if depreciation_row.height > 0:
                    depreciation = depreciation_row.select(pl.col(total_col)).item(0, 0)
                    if depreciation:
                        ebitda = calc.calculate_ebitda(operating_income, abs(depreciation))
                        calculations.append(
                            CalculationResult(
                                name="EBITDA",
                                value=ebitda,
                                formula="Operating Income + Depreciation",
                                year=year,
                            )
                        )
                        ebitda_margin = calc.calculate_operating_margin(ebitda, revenue)
                        if ebitda_margin is not None:
                            calculations.append(
                                CalculationResult(
                                    name="EBITDA Margin %",
                                    value=ebitda_margin,
                                    formula="(EBITDA / Revenue) * 100",
                                    year=year,
                                )
                            )

        financial_row = subtotal_data.filter(pl.col("Categorie") == "Financiële baten en lasten")
        if financial_row.height > 0:
            financial_result = financial_row.select(pl.col(total_col)).item(0, 0)
            if financial_result is not None:
                calculations.append(
                    CalculationResult(
                        name="Net Financial Result",
                        value=financial_result,
                        formula="Interest Income - Interest Expenses + Other Financial Results",
                        year=year,
                    )
                )

    # Revenue growth (most recent year only)
    if len(year_columns) >= 2:
        current_year = max(year_columns)
        previous_year = str(int(current_year) - 1)

        if f"{previous_year}_total" in subtotal_data.columns:
            revenue_rows = subtotal_data.filter(pl.col("Categorie") == "Bedrijfsopbrengsten")
            if revenue_rows.height > 0:
                try:
                    rev_current = revenue_rows.select(pl.col(f"{current_year}_total")).item(0, 0)
                    rev_previous = revenue_rows.select(pl.col(f"{previous_year}_total")).item(0, 0)
                    if rev_current is not None and rev_previous is not None:
                        growth = calc.calculate_revenue_growth(rev_current, rev_previous)
                        if growth is not None:
                            calculations.append(
                                CalculationResult(
                                    name="Revenue Growth %",
                                    value=growth,
                                    formula=(
                                        f"(({current_year} Revenue - {previous_year} Revenue)"
                                        f" / {previous_year} Revenue) * 100"
                                    ),
                                    year=current_year,
                                )
                            )
                except (IndexError, ValueError) as e:
                    print(f"Warning: Could not calculate revenue growth: {e}")

    # Personnel cost ratio
    for year in year_columns:
        total_col = f"{year}_total"
        if total_col not in subtotal_data.columns:
            continue

        revenue_row = subtotal_data.filter(pl.col("Categorie") == "Bedrijfsopbrengsten")
        personnel_row = subtotal_data.filter(pl.col("Categorie") == "Personeelskosten")

        if revenue_row.height > 0 and personnel_row.height > 0:
            try:
                revenue = revenue_row.select(pl.col(total_col)).item(0, 0)
                personnel_costs = personnel_row.select(pl.col(total_col)).item(0, 0)
                if revenue and personnel_costs and revenue > 0:
                    personnel_ratio = (abs(personnel_costs) / revenue) * 100
                    calculations.append(
                        CalculationResult(
                            name="Personnel Cost Ratio %",
                            value=personnel_ratio,
                            formula="(Personnel Costs / Revenue) * 100",
                            year=year,
                        )
                    )
            except (IndexError, ValueError) as e:
                print(f"Warning: Could not calculate personnel cost ratio for {year}: {e}")

    # Balance-sheet-based ratios
    balance_sheet_data = (
        df.filter(pl.col("Balance_Side").is_not_null())
        if "Balance_Side" in df.columns
        else pl.DataFrame()
    )

    if balance_sheet_data.height > 0:
        income_data = df.filter(pl.col("row_type") == "original")
        net_income_sources = ["Resultaat na belastingen", "Net Income", "Netto resultaat"]

        for year in year_columns:
            total_col = f"{year}_total"

            net_income: Optional[float] = None
            for source in net_income_sources:
                income_row = income_data.filter(pl.col("Categorie") == source)
                if income_row.height > 0 and total_col in income_row.columns:
                    net_income = income_row.select(pl.col(total_col)).item(0, 0)
                    break

            assets_row = balance_sheet_data.filter(
                (pl.col("Balance_Side") == "Activa") & (pl.col("row_type") == "subtotal")
            )
            if assets_row.height > 0 and net_income is not None and total_col in assets_row.columns:
                try:
                    total_assets = assets_row.select(pl.col(total_col)).item(0, 0)
                    if total_assets and total_assets > 0:
                        roa = (net_income / total_assets) * 100
                        calculations.append(
                            CalculationResult(
                                name="Return on Assets %",
                                value=roa,
                                formula="(Net Income / Total Assets) * 100",
                                year=year,
                            )
                        )
                except (IndexError, ValueError) as e:
                    print(f"Warning: Could not calculate ROA for {year}: {e}")

        for year in year_columns:
            total_col = f"{year}_total"
            try:
                ca_row = balance_sheet_data.filter(
                    (pl.col("Level0") == "Vlottende activa") & (pl.col("row_type") == "subtotal")
                )
                cl_row = balance_sheet_data.filter(pl.col("Categorie") == "Kortlopende schulden")
                if (
                    ca_row.height > 0
                    and cl_row.height > 0
                    and total_col in ca_row.columns
                    and total_col in cl_row.columns
                ):
                    current_assets = ca_row.select(pl.col(total_col)).item(0, 0)
                    current_liabilities = cl_row.select(pl.col(total_col)).item(0, 0)
                    if current_assets is not None and current_liabilities is not None:
                        working_capital = current_assets - current_liabilities
                        calculations.append(
                            CalculationResult(
                                name="Working Capital",
                                value=working_capital,
                                formula="Current Assets - Current Liabilities",
                                year=year,
                            )
                        )
                        if current_liabilities != 0:
                            cr = FinancialCalculations.calculate_current_ratio(
                                current_assets, current_liabilities
                            )
                            if cr is not None:
                                calculations.append(
                                    CalculationResult(
                                        name="Current Ratio",
                                        value=cr,
                                        formula="Current Assets / Current Liabilities",
                                        year=year,
                                    )
                                )
            except (IndexError, ValueError) as e:
                print(f"Warning: Could not calculate working capital for {year}: {e}")

    return calculations


__all__ = [
    "CalculationResult",
    "FinancialCalculations",
    "RuleStatus",
    "BusinessRuleResult",
    "DutchGAAPBusinessRules",
    "apply_calculations_with_pivot_results",
]
