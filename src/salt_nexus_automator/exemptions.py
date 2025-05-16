# src/exemptions.py
import pandas as pd
import logging
import os
from .utils import ErrorCollector

class ExemptionManager:
    """Handles tagging and filtering of exempt transactions."""

    def __init__(self, sales_df: pd.DataFrame, error_collector: ErrorCollector,
                 exempt_customer_csv: str = None, exempt_invoice_csv: str = None):
        """
        Initializes the ExemptionManager.

        Args:
            sales_df (pd.DataFrame): The standardized sales data (should have undergone standardization).
            error_collector (ErrorCollector): Instance for collecting errors/warnings.
            exempt_customer_csv (str, optional): Path to supplemental exempt customer list. Defaults to None.
            exempt_invoice_csv (str, optional): Path to supplemental exempt invoice list. Defaults to None.
        """
        self.sales_df = sales_df.copy() # Work on a copy
        self.error_collector = error_collector
        self.exempt_customer_csv = exempt_customer_csv
        self.exempt_invoice_csv = exempt_invoice_csv

        # Initialize 'is_exempt' column if it doesn't exist
        if 'is_exempt' not in self.sales_df.columns:
            self.sales_df['is_exempt'] = False
        else:
            # Ensure existing column is boolean, default False if NaN
            self.sales_df['is_exempt'] = self.sales_df['is_exempt'].fillna(False).astype(bool)

        logging.info("ExemptionManager initialized.")


    def _load_exempt_list(self, file_path: str, column_name: str) -> set:
        """Loads exempt IDs/numbers from a CSV file, reporting errors."""
        exempt_set = set()
        if file_path and os.path.exists(file_path):
            logging.info(f"Loading exemption list from: {file_path} using column '{column_name}'")
            try:
                df = pd.read_csv(file_path)
                if column_name not in df.columns:
                    msg = f"Exemption file {file_path} missing required column '{column_name}'. Cannot apply these exemptions."
                    self.error_collector.add_warning(msg)
                    logging.warning(msg)
                else:
                    # Convert to string for consistent matching, drop NaNs/empty strings
                    exempt_items = df[column_name].dropna().astype(str).str.strip()
                    exempt_set = set(exempt_items[exempt_items != ''])
                    logging.info(f"Loaded {len(exempt_set)} unique exempt identifiers from {file_path}.")
            except Exception as e:
                 msg = f"Error loading or processing exemption file {file_path}: {e}"
                 self.error_collector.add_warning(msg)
                 logging.error(msg, exc_info=True)
        elif file_path:
             # File path provided but not found
             msg = f"Exemption file not found: {file_path}. Skipping this exemption source."
             self.error_collector.add_warning(msg)
             logging.warning(msg)
        return exempt_set

    def apply_exemptions(self) -> pd.DataFrame:
        """
        Applies exemption rules based on source data columns and supplemental files.

        Returns:
            pd.DataFrame: Sales DataFrame with 'is_exempt' column updated.
        """
        logging.info("Applying exemption rules...")
        if self.sales_df.empty:
            logging.warning("Sales DataFrame is empty. Skipping exemption application.")
            return self.sales_df

        initial_exempt_count = self.sales_df['is_exempt'].sum()
        current_exempt_mask = self.sales_df['is_exempt'].copy()

        # 1. Check for existing exemption indicator columns in source data
        # Use existing 'is_exempt' column value if present and True
        logging.debug(f"Initial exemptions from source 'is_exempt' column: {current_exempt_mask.sum()}")


        # Check for taxability code (example using common exempt values)
        if 'taxability_code' in self.sales_df.columns:
             # TODO: Define exempt codes more robustly, possibly in config?
             exempt_codes = ['EXEMPT', 'RESALE', 'WHOLESALE', 'GOVERNMENT', 'NONTAXABLE'] # Example codes
             # Handle potential NaN values in the column before string operations
             code_exempt_mask = self.sales_df['taxability_code'].fillna('').astype(str).str.upper().isin(exempt_codes)
             newly_exempted_by_code = code_exempt_mask.sum()
             current_exempt_mask |= code_exempt_mask # Combine with existing mask using OR
             logging.info(f"Applied exemptions based on 'taxability_code': {newly_exempted_by_code} rows marked.")

        # 2. Apply exemptions based on supplemental customer/invoice CSV files
        # Determine customer identifier column (e.g., 'customer_id' or 'customer_name')
        # TODO: Make the column name used for matching configurable?
        customer_match_col = 'customer_id' if 'customer_id' in self.sales_df.columns else 'customer_name'
        exempt_customers = self._load_exempt_list(self.exempt_customer_csv, customer_match_col)

        exempt_invoices = self._load_exempt_list(self.exempt_invoice_csv, 'invoice_number')

        if exempt_customers and customer_match_col in self.sales_df.columns:
             cust_exempt_mask = self.sales_df[customer_match_col].astype(str).isin(exempt_customers)
             newly_exempted_by_cust = cust_exempt_mask.sum()
             current_exempt_mask |= cust_exempt_mask
             logging.info(f"Applied exemptions based on Customer List CSV using column '{customer_match_col}': {newly_exempted_by_cust} rows marked.")
        elif exempt_customers:
             logging.warning(f"Could not apply customer exemptions: Column '{customer_match_col}' not found in sales data.")


        if exempt_invoices and 'invoice_number' in self.sales_df.columns:
             inv_exempt_mask = self.sales_df['invoice_number'].astype(str).isin(exempt_invoices)
             newly_exempted_by_inv = inv_exempt_mask.sum()
             current_exempt_mask |= inv_exempt_mask
             logging.info(f"Applied exemptions based on Invoice List CSV: {newly_exempted_by_inv} rows marked.")

        # Apply the final combined mask
        self.sales_df['is_exempt'] = current_exempt_mask
        final_exempt_count = self.sales_df['is_exempt'].sum()
        total_newly_marked = final_exempt_count - initial_exempt_count

        logging.info(f"Exemption application complete. Total exempt transactions: {final_exempt_count} ({total_newly_marked} newly marked via rules/lists).")
        return self.sales_df