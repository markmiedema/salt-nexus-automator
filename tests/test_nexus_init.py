from salt_nexus_automator.nexus_analysis import NexusAnalyzer

def test_nexus_analyzer_init_runs():
    cfg = {"CA": {"sales_threshold": 100000, "lookback_rule": "rolling_12m"}}
    analyzer = NexusAnalyzer(cfg)          # should not raise
    # minimal DataFrame with required cols
    import pandas as pd
    df = pd.DataFrame({
        "invoice_date": ["2024-01-01"],
        "state": ["CA"],
        "channel": ["DTC"],
        "total_amount": [10],
        "invoice_number": ["X"],
    })
    result = analyzer.analyze_nexus(df)
    assert not result.empty
