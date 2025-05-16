import pandas as pd
# Assuming DataLoader is not actually used in this specific test,
# but keeping imports as they were provided in the previous turn.
# from salt_nexus_automator.ingestion import DataLoader
from salt_nexus_automator.exposure_calc import ExposureCalculator # Corrected import

# Corrected test function based on pytest output
def test_exposure_accepts_no_datetime_index():
    # Minimal configuration for a state
    cfg = {"CA": {"sales_threshold": 100000, "lookback_rule": "rolling_12m",
                  "tax_rate": 0.07}}

    # Sample sales data without a DatetimeIndex, but with 'invoice_date' and 'month_year'
    sales = pd.DataFrame({
        "invoice_date": ["2024-01-01", "2024-02-15", "2024-01-20"],
        "state": ["CA", "CA", "NY"],
        "channel": ["DTC", "Marketplace", "DTC"],
        "total_amount": [10, 25, 15],
        "invoice_number": ["X1", "Y2", "Z3"],
        "month_year": ["2024-01", "2024-02", "2024-01"], # String representation
        "is_exempt": [False, False, True], # Include exempt sales
    })

    # Sample nexus summary data with string representation of month
    nexus_summary = pd.DataFrame({
        "state": ["CA", "NY"],
        "first_trigger_month": ["2024-01", "2024-03"], # String representation
        "sales_threshold_met": [True, False], # Example additional columns
        "txns_threshold_met": [False, False],
    })

    # Initialize ExposureCalculator with required arguments
    # Added error_collector=None to fix the TypeError
    calc = ExposureCalculator(cfg, "Estimate", error_collector=None)

    # Call the method that should handle non-DatetimeIndex sales_df
    # This should NOT raise a TypeError due to the index handling logic
    df_exposure = calc.calculate_exposure(sales, nexus_summary)

    # Assertions to check the output (adjust based on expected results)
    # The previous test failed because df_exposure was empty.
    # Let's assert it's NOT empty if we expect exposure for CA after Jan 2024.
    # CA triggered in 2024-01, exposure calculation starts from 2024-02.
    # Only one taxable CA sale in 2024-02 in the sample data (total_amount 25).
    # NY did not trigger nexus in the sample summary.
    assert not df_exposure.empty
    assert "state" in df_exposure.columns
    assert "month_year" in df_exposure.columns
    assert "estimated_tax" in df_exposure.columns
    # Check for expected states and months with exposure
    assert "CA" in df_exposure["state"].unique()
    assert "NY" not in df_exposure["state"].unique() # NY trigger is 2024-03, no sales after that in sample
    assert pd.Period("2024-02", freq="M") in df_exposure["month_year"].unique()
    assert pd.Period("2024-01", freq="M") not in df_exposure["month_year"].unique() # Exposure starts *after* trigger month

    # Optional: Check the calculated tax for a specific month/state
    ca_feb_exposure = df_exposure[(df_exposure["state"] == "CA") & (df_exposure["month_year"] == pd.Period("2024-02", freq="M"))]
    assert not ca_feb_exposure.empty
    # Taxable sales for CA in Feb 2024 is 25, tax rate is 0.07
    expected_tax = 25 * 0.07
    # Use a tolerance for floating point comparison
    assert abs(ca_feb_exposure["estimated_tax"].iloc[0] - expected_tax) < 1e-9
