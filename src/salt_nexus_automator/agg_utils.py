# src/agg_utils.py
"""Vectorised aggregation helpers for Economic-Nexus analysis."""

from __future__ import annotations
import os
from typing import Tuple

import pandas as pd

__all__ = [
    "monthly_state_summary",
    "add_rolling_12m",
    "add_calendar_year_metrics",
    "evaluate_thresholds",
    "MARKETPLACE_CHANNELS",
]

# -- Marketplace channels ----------------------------------------------------
MARKETPLACE_CHANNELS = {
    ch.strip().upper()
    for ch in os.getenv("MARKETPLACE_CHANNELS", "AMAZON-FBA,ETSY,EBAY").split(",")
}

# --------------------------------------------------------------------------- #
# 1. Raw → monthly summary                                                    #
# --------------------------------------------------------------------------- #
def monthly_state_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates raw sales data into a monthly summary by state.

    The output DataFrame has a MultiIndex of ['state', 'month_year'].
    """
    # Ensure month_year is Period[M] before grouping
    if "month_year" not in df.columns or not pd.api.types.is_period_dtype(df["month_year"]):
         # Assuming invoice_date is available and is a DatetimeIndex or can be converted
         if not isinstance(df.index, pd.DatetimeIndex) and "invoice_date" in df.columns:
              df = df.copy() # Avoid SettingWithCopyWarning
              df["invoice_date"] = pd.to_datetime(df["invoice_date"])
              df = df.set_index("invoice_date")

         if isinstance(df.index, pd.DatetimeIndex):
             df["month_year"] = df.index.to_period("M")
         else:
             # If month_year is missing and index isn't DatetimeIndex, raise error
             raise TypeError("DataFrame must have 'month_year' column or a DatetimeIndex.")


    g = (
        df.groupby(["state", "month_year"], sort=True)
        .agg(
            sales=("total_amount", "sum"),
            txns=("invoice_number", "nunique"),
        )
        # The result of groupby on columns is a DataFrame with a MultiIndex.
        # We want to keep this MultiIndex as ['state', 'month_year'].
        # No reset_index() is needed here before setting the index, as groupby already creates it.
    )
    # Ensure the index names are explicitly set after groupby
    g.index.names = ["state", "month_year"]

    # Ensure 'cy' column is created from 'month_year' Period data
    # Corrected: Access .year directly on the PeriodIndex level (removed .dt)
    g["cy"] = g.index.get_level_values("month_year").year.astype("int16") # Get year from the index level
    return g


# --------------------------------------------------------------------------- #
# 2. Rolling 12-month totals                                                  #
# --------------------------------------------------------------------------- #
def add_rolling_12m(df_m: pd.DataFrame, *, window: int = 12) -> pd.DataFrame:
    """
    Append rolling-12-month totals (sales_12m / txns_12m).

    * ``df_m`` must already be indexed by ('state', 'month_year').
    * Returns the same frame with two new columns.
    """
    # sanity check
    if not isinstance(df_m.index, pd.MultiIndex) or list(df_m.index.names) != ["state", "month_year"]:
        raise TypeError("Input must use MultiIndex ['state', 'month_year'].")

    # transform → result is aligned 1-for-1 with the original index,
    # so there is no extra level to drop and no column overlap.
    # Using transform applies the rolling sum within each group and returns
    # a DataFrame with the same index as the original df_m.
    rolled = (
        df_m.groupby(level=0)[["sales", "txns"]]
        .transform(lambda s: s.rolling(window, min_periods=1).sum())
        .rename(columns={"sales": "sales_12m", "txns": "txns_12m"})
    )

    # indexes now match exactly → join is safe
    return df_m.join(rolled)


# --------------------------------------------------------------------------- #
# 3. Calendar-year helper metrics                                             #
# --------------------------------------------------------------------------- #
def add_calendar_year_metrics(df_m: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate previous year and current year-to-date metrics.

    Expects input DataFrame `df_m` to have a MultiIndex of ['state', 'month_year']
    and a 'cy' column.
    """
    # Ensure input has the expected MultiIndex and 'cy' column
    if not isinstance(df_m.index, pd.MultiIndex) or list(df_m.index.names) != ["state", "month_year"]:
         raise TypeError("Input DataFrame must have a MultiIndex of ['state', 'month_year'].")
    if "cy" not in df_m.columns:
         # This check might be redundant if monthly_state_summary always adds 'cy'
         # but keeping it for safety.
         # Attempt to create 'cy' from index if missing
         try:
             # Corrected: Access .year directly on the PeriodIndex level (removed .dt)
             df_m["cy"] = df_m.index.get_level_values("month_year").year.astype("int16")
         except Exception as e:
             raise TypeError("Input DataFrame must contain a 'cy' column or have 'month_year' in index.") from e


    # Calculate previous year totals by grouping by state and year, then shifting
    prev_totals = (
        df_m.groupby(level="state")[["sales", "txns"]].sum() # Group by state level, sum sales/txns
            .groupby(level="state")[["sales", "txns"]] # Group again by state level for shift
            .shift(1) # Shift by one year within each state group (assuming annual grouping)
            .rename(columns={"sales": "sales_prev_yr", "txns": "txns_prev_yr"})
    )
    # The index of prev_totals will be ['state', 'cy'] after the sum and shift

    # Calculate current year-to-date totals by grouping by state and using cumulative sum
    curr_ytd = (
        df_m.groupby(level="state")[["sales", "txns"]] # Group by state level
            .cumsum() # Cumulative sum within each state group
            .rename(columns={"sales": "sales_curr_ytd", "txns": "txns_curr_ytd"})
    )
    # The index of curr_ytd will be ['state', 'month_year']

    # Join the calculated metrics back to the original monthly summary.
    # Joining prev_totals (indexed by ['state', 'cy']) to df_m (indexed by ['state', 'month_year'])
    # requires pandas to align based on the shared 'state' level and implicitly 'cy' from df_m's column.
    # Joining curr_ytd works directly on the MultiIndex ['state', 'month_year'].
    # A simple join on df_m's index should work.
    return df_m.join([prev_totals, curr_ytd])

# --------------------------------------------------------------------------- #
# 4. Threshold evaluator                                                      #
# --------------------------------------------------------------------------- #
def evaluate_thresholds(
    df_v: pd.DataFrame,
    rule: str,
    sales_th: float | None,
    txn_th: int | None,
) -> pd.DataFrame:
    """
    Evaluates if sales or transaction thresholds are met based on a rule.

    Expects input DataFrame `df_v` to contain the necessary columns
    ('sales_12m', 'txns_12m', 'sales_prev_yr', etc.) based on the rule.
    """
    if rule == "calendar_prev_curr":
        # Use max of previous calendar year total and current year-to-date
        sales_base = df_v[["sales_prev_yr", "sales_curr_ytd"]].max(axis=1)
        txns_base  = df_v[["txns_prev_yr",  "txns_curr_ytd"] ].max(axis=1)
    elif rule == "calendar_prev":
        # Use previous calendar year total
        sales_base = df_v["sales_prev_yr"]
        txns_base  = df_v["txns_prev_yr"]
    else:  # default -> rolling_12m
        # Use rolling 12-month totals
        sales_base = df_v["sales_12m"]
        txns_base  = df_v["txns_12m"]

    # Evaluate thresholds
    sales_met = (sales_base >= sales_th) if sales_th is not None else False
    txn_met = (txns_base  >= txn_th)  if txn_th  is not None else False

    return pd.DataFrame(
        {
            "sales_met": sales_met,
            "txn_met": txn_met,
        },
        index=df_v.index, # Preserve the original index (should be MultiIndex)
    )
