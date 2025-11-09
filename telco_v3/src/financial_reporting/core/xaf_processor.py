"""
XAF File Processing Module

Handles XML Audit File (XAF) format processing for Dutch accounting data.
"""

import polars as pl
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

try:
    from version import __version__
except ImportError:
    __version__ = "0.3.0"


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


__all__ = ["XAFProcessor"]
