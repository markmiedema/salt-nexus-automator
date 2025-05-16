import pandas as pd
from salt_nexus_automator.ingestion import DataLoader
# Assuming ErrorCollector is available if needed for DataLoader
# from .utils import ErrorCollector

def test_sales_channel_renamed():
    # Added all required columns to the raw DataFrame for _postprocess validation
    df_raw = pd.DataFrame({
        "date": ["2024-01-01"],
        "invoice_date": ["2024-01-01"],
        "invoice_number": ["INV-1"],
        "total_amount": [100.0],
        "customer_name": ["Test Customer"],
        "street_address": ["123 Main St"],
        "city": ["Anytown"],
        "state": ["CA"],
        "zip_code": ["90210"],
        "sales_channel": ["Marketplace"], # This is a required column that gets renamed
        # Optional columns are not strictly needed for the required column check
        # "is_exempt": [False],
        # "taxability_code": ["TAXABLE"],
        # "customer_id": ["C123"],
    })
    # The DataLoader __init__ method requires error_collector
    # Initializing DataLoader with a dummy file path and error_collector=None
    # If ErrorCollector is needed, instantiate it: error_collector=ErrorCollector()
    dl = DataLoader("", error_collector=None)
    # Calling _postprocess with the raw DataFrame and a source_name
    df_clean = dl._postprocess(df_raw, source_name="test_data")
    # Assert that the column was renamed correctly
    assert "channel" in df_clean.columns
    assert "sales_channel" not in df_clean.columns
    # Assert that the DataFrame is not empty after processing
    assert not df_clean.empty
