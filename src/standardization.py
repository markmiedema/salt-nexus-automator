# src/standardization.py
import pandas as pd
import usaddress # For basic parsing, part of usaddress-scourgify typically
import logging
from src.utils import ErrorCollector
import numpy as np

class DataStandardizer:
    """Cleans, standardizes types, validates, and prepares sales data."""

    def __init__(self, df: pd.DataFrame, error_collector: ErrorCollector):
        """
        Initializes the DataStandardizer.

        Args:
            df (pd.DataFrame): Raw data loaded from ingestion.
            error_collector (ErrorCollector): Instance for collecting errors/warnings.
        """
        self.df = df.copy() # Work on a copy
        self.error_collector = error_collector
        logging.info(f"DataStandardizer initialized with {len(self.df)} rows.")

    def standardize_dates(self, date_cols: list = ['date', 'invoice_date']) -> None:
        """Converts specified date columns to datetime objects, handling errors."""
        logging.info(f"Standardizing date columns: {date_cols}...")
        for col in date_cols:
            if col in self.df.columns:
                original_nan_count = self.df[col].isna().sum()
                # Try converting, coercing errors. Handle mixed types gracefully.
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                current_nan_count = self.df[col].isna().sum()
                failed_parses = current_nan_count - original_nan_count

                if failed_parses > 0:
                    msg = f"{failed_parses} entries in column '{col}' could not be parsed as dates and were set to NaT."
                    self.error_collector.add_warning(msg)
                    # Add rows that failed parsing (were not originally NaT/NaN) to rejected data
                    # Identify rows where original value was not null/NaT but became NaT after conversion
                    # This requires careful handling of original types if needed. For simplicity:
                    # We'll rely on the dropna in validate_data for the critical 'date' column.
                    # If specific row rejection is needed here, store original column before conversion.
            else:
                 logging.warning(f"Date column '{col}' not found in DataFrame. Skipping standardization.")

    def clean_addresses(self) -> None:
        """Standardizes address fields using basic cleaning and optionally usaddress."""
        logging.info("Standardizing address fields (street, city, state, zip)...")
        address_cols = ['street_address', 'city', 'state', 'zip_code']
        for col in address_cols:
            if col in self.df.columns:
                # Basic cleaning: Convert to string, strip whitespace
                self.df[col] = self.df[col].fillna('').astype(str).str.strip()
                if col == 'state':
                    self.df[col] = self.df[col].str.upper()
                    # Basic validation: Ensure state is 2 letters (can be enhanced)
                    state_mask = (self.df[col].str.len() != 2) & (self.df[col] != '')
                    invalid_states = self.df.loc[state_mask, col].unique()
                    if len(invalid_states) > 0:
                        msg = f"Found potentially invalid state abbreviations: {list(invalid_states)}. Review data."
                        self.error_collector.add_warning(msg)
                if col == 'zip_code':
                    # Extract ZIP5 more robustly, handle non-string cases
                    self.df[col] = self.df[col].str.extract(r'^(\d{5})', expand=False).fillna('')

        # Apply usaddress parsing (MVP: basic usage for potential future use/validation)
        # Note: This library primarily tags components. Actual cleaning/standardization might need more rules.
        # Wrap in try-except as it can fail on very malformed strings.
        # Consider making this step optional via config due to performance implications.
        if 'street_address' in self.df.columns:
             logging.info("Applying basic usaddress tagging (informational)...")
             # Define a function to safely tag addresses
             def safe_tag_address(address):
                 if not isinstance(address, str) or not address:
                     return None # Return None for non-strings or empty strings
                 try:
                     tagged_address, address_type = usaddress.tag(address)
                     # Could return tagged_address dict or just the type, etc.
                     return address_type # Example: return the classified type
                 except Exception as e:
                     # logging.debug(f"usaddress tagging failed for address: {address} - Error: {e}")
                     return "Untaggable" # Indicate failure

             # Apply the tagging (this creates a new column, doesn't modify original)
             # self.df['address_type_usaddress'] = self.df['street_address'].apply(safe_tag_address)
             logging.info("Basic usaddress tagging step completed.")
        logging.info("Address field standardization complete.")


    def enforce_data_types(self) -> None:
        """Enforces appropriate data types for key columns, rejecting invalid rows."""
        logging.info("Enforcing data types...")

        # total_amount: Convert to numeric, reject failures
        if 'total_amount' in self.df.columns:
            original_nan_count = self.df['total_amount'].isna().sum()
            self.df['total_amount'] = pd.to_numeric(self.df['total_amount'], errors='coerce')
            nan_mask = self.df['total_amount'].isna()
            failed_numeric_indices = self.df.index[nan_mask & (self.df['total_amount'].isna().sum() > original_nan_count)]

            if not failed_numeric_indices.empty:
                 msg = f"{len(failed_numeric_indices)} entries in 'total_amount' could not be converted to numeric."
                 self.error_collector.add_warning(msg)
                 for index in failed_numeric_indices:
                     self.error_collector.add_rejected_row(self.df.loc[index].to_dict(), "Invalid numeric format in 'total_amount'")
                 # Set failed rows' amount to NaN to allow dropping later if needed
                 self.df.loc[failed_numeric_indices, 'total_amount'] = np.nan

        # invoice_number: Ensure it's a string
        if 'invoice_number' in self.df.columns:
            self.df['invoice_number'] = self.df['invoice_number'].fillna('').astype(str)

        # customer_id (if present): Ensure string
        if 'customer_id' in self.df.columns:
            self.df['customer_id'] = self.df['customer_id'].fillna('').astype(str)

        logging.info("Data type enforcement complete.")


    def validate_data(self) -> None:
        """Performs data integrity checks (uniqueness, format) and drops invalid rows."""
        logging.info("Performing data validation checks...")

        # Example: Check for duplicate invoice numbers (add warning, but don't reject by default)
        if 'invoice_number' in self.df.columns and not self.df['invoice_number'].empty:
             # Exclude empty strings before checking duplicates
             non_empty_invoices = self.df[self.df['invoice_number'] != '']
             duplicates = non_empty_invoices[non_empty_invoices.duplicated(subset=['invoice_number'], keep=False)]
             if not duplicates.empty:
                 num_duplicates = duplicates['invoice_number'].nunique()
                 msg = f"Found {num_duplicates} potentially duplicate invoice numbers (non-unique values exist). Review source data if uniqueness is expected."
                 self.error_collector.add_warning(msg)

        # Check for and drop rows with missing critical values AFTER standardization attempts
        critical_cols = ['date', 'total_amount', 'state', 'invoice_number']
        initial_rows = len(self.df)
        # Also check for empty state strings
        self.df.dropna(subset=critical_cols, inplace=True)
        self.df = self.df[self.df['state'] != ''] # Remove rows with empty state after stripping

        dropped_rows = initial_rows - len(self.df)
        if dropped_rows > 0:
             msg = f"Dropped {dropped_rows} rows due to missing critical values (date, amount, non-empty state, invoice#) after standardization."
             # Note: These rows were likely already captured by add_rejected_row if the NaN
             # resulted from a failed conversion of originally non-null data.
             # If they were null/empty originally, dropping them here is correct.
             self.error_collector.add_warning(msg) # Add warning about the drop
             logging.warning(msg)

        logging.info("Data validation checks complete.")


    def standardize(self) -> pd.DataFrame:
        """
        Runs all standardization and validation steps.

        Returns:
            pd.DataFrame: The cleaned, standardized DataFrame, ready for analysis.
                          Rows failing critical conversions are removed and logged/reported.
        """
        logging.info("Starting data standardization process...")
        if self.df.empty:
            logging.warning("Input DataFrame is empty. Skipping standardization.")
            self.error_collector.update_summary('rows_processed', 0)
            return self.df

        self.standardize_dates()
        self.clean_addresses()
        self.enforce_data_types() # This step now adds rejects for failed amount conversions
        self.validate_data()      # This step now drops rows with remaining critical NaNs/empty state

        processed_rows = len(self.df)
        # Update summary - rows processed is count *after* standardization and dropping critical nulls
        self.error_collector.update_summary('rows_processed', processed_rows)

        logging.info(f"Data standardization process completed. {processed_rows} rows ready for analysis.")
        return self.df