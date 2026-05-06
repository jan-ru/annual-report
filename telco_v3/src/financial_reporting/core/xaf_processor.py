"""
XAF File Processing Module

Handles XML Audit File (XAF) format processing for Dutch accounting data.
Uses defusedxml to prevent XML bomb / XXE attacks on untrusted input.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import defusedxml.ElementTree as ET
import polars as pl

try:
    from version import __version__
except ImportError:
    __version__ = "0.3.0"


class XAFProcessor:
    """Parse an XAF (XML Audit File) and extract accounting data into Polars DataFrames."""

    def __init__(self, xaf_file_path: str | Path) -> None:
        self.xaf_file_path = Path(xaf_file_path)
        self.tree: Any = None
        self.root: Any = None
        self.namespace: Dict[str, str] = {}

    def load_xaf_file(self) -> bool:
        """Load and parse the XAF file. Returns True on success."""
        try:
            self.tree = ET.parse(str(self.xaf_file_path))
            self.root = self.tree.getroot()

            if self.root.tag.startswith("{"):
                namespace_uri = self.root.tag.split("}")[0][1:]
                self.namespace = {"xaf": namespace_uri}

            return True
        except Exception as e:
            print(f"Error loading XAF file: {e}")
            return False

    # ------------------------------------------------------------------
    # Data extraction
    # ------------------------------------------------------------------

    def extract_chart_of_accounts(self) -> pl.DataFrame:
        """Return chart of accounts as a Polars DataFrame."""
        accounts: List[Dict[str, Any]] = []

        elements = (
            self.root.findall(".//xaf:account", self.namespace)
            if self.namespace
            else self.root.findall(".//account")
        )
        for account in elements:
            accounts.append(
                {
                    "account_id": self._get_text(account, "accountID"),
                    "account_desc": self._get_text(account, "accountDesc"),
                    "account_type": self._get_text(account, "accountType"),
                    "account_class": self._get_text(account, "accountClass"),
                    "opening_balance_debit": self._get_float(account, "openingBalanceDebit"),
                    "opening_balance_credit": self._get_float(account, "openingBalanceCredit"),
                }
            )

        return pl.DataFrame(accounts)

    def extract_transactions(self) -> pl.DataFrame:
        """Return all transaction lines as a Polars DataFrame."""
        transactions: List[Dict[str, Any]] = []

        tx_elements = (
            self.root.findall(".//xaf:transaction", self.namespace)
            if self.namespace
            else self.root.findall(".//transaction")
        )
        for tx in tx_elements:
            trans_id = self._get_text(tx, "transactionID")
            trans_date = self._get_text(tx, "transactionDate")
            description = self._get_text(tx, "description")

            lines = (
                tx.findall(".//xaf:line", self.namespace)
                if self.namespace
                else tx.findall(".//line")
            )
            for line in lines:
                transactions.append(
                    {
                        "transaction_id": trans_id,
                        "transaction_date": trans_date,
                        "description": description,
                        "line_number": self._get_text(line, "lineNumber"),
                        "account_id": self._get_text(line, "accountID"),
                        "debit_amount": self._get_float(line, "debitAmount"),
                        "credit_amount": self._get_float(line, "creditAmount"),
                        "line_description": self._get_text(line, "lineDescription"),
                    }
                )

        return pl.DataFrame(transactions)

    def extract_trial_balance(self) -> pl.DataFrame:
        """Return trial balance data as a Polars DataFrame."""
        rows: List[Dict[str, Any]] = []

        tb_elements = (
            self.root.findall(".//xaf:trialBalance", self.namespace)
            if self.namespace
            else self.root.findall(".//trialBalance")
        )
        for tb_line in tb_elements:
            rows.append(
                {
                    "account_id": self._get_text(tb_line, "accountID"),
                    "account_desc": self._get_text(tb_line, "accountDesc"),
                    "opening_balance_debit": self._get_float(tb_line, "openingBalanceDebit"),
                    "opening_balance_credit": self._get_float(tb_line, "openingBalanceCredit"),
                    "turnover_debit": self._get_float(tb_line, "turnoverDebit"),
                    "turnover_credit": self._get_float(tb_line, "turnoverCredit"),
                    "closing_balance_debit": self._get_float(tb_line, "closingBalanceDebit"),
                    "closing_balance_credit": self._get_float(tb_line, "closingBalanceCredit"),
                }
            )

        return pl.DataFrame(rows)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_text(self, parent: Any, tag_name: str) -> Optional[str]:
        """Return text of first matching child element, or None."""
        if self.namespace:
            element = parent.find(f".//xaf:{tag_name}", self.namespace)
        else:
            element = parent.find(f".//{tag_name}")
        return element.text if element is not None else None

    def _get_float(self, parent: Any, tag_name: str) -> Optional[float]:
        """Return float value of first matching child element, or None."""
        text = self._get_text(parent, tag_name)
        try:
            return float(text) if text else None
        except (ValueError, TypeError):
            return None

    # Keep old method names as aliases for backward compatibility
    def _get_element_text(self, parent: Any, tag_name: str) -> Optional[str]:
        return self._get_text(parent, tag_name)

    def _get_element_float(self, parent: Any, tag_name: str) -> Optional[float]:
        return self._get_float(parent, tag_name)


__all__ = ["XAFProcessor"]
