# src/agg_utils.py
"""Vectorised aggregation helpers for Economic‑Nexus analysis."""

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
    g = (
        df.groupby(["state", "month_year"], sort=True)
          .agg(
              sales=("total_amount", "sum"),
              txns=("invoice_number", "nunique"),
          )
          .reset_index()
    )
    g["cy"] = g["month_year"].dt.year.astype("int16")
    return g.set_index(["state", "month_year"])

# --------------------------------------------------------------------------- #
# 2. Rolling 12‑month totals                                                  #
# --------------------------------------------------------------------------- #
def add_rolling_12m(df_m: pd.DataFrame, *, window: int = 12) -> pd.DataFrame:
    rolled = (
        df_m.groupby(level=0, group_keys=False)[["sales", "txns"]]
            .rolling(window=window, min_periods=1)
            .sum()
            .rename(columns={"sales": "sales_12m", "txns": "txns_12m"})
    )
    return df_m.join(rolled)

# --------------------------------------------------------------------------- #
# 3. Calendar‑year helper metrics                                             #
# --------------------------------------------------------------------------- #
def add_calendar_year_metrics(df_m: pd.DataFrame) -> pd.DataFrame:
    prev_totals = (
        df_m.groupby(["state", "cy"])[["sales", "txns"]].sum()
            .groupby(level=0)[["sales", "txns"]]
            .shift(1)
            .rename(columns={"sales": "sales_prev_yr", "txns": "txns_prev_yr"})
    )

    curr_ytd = (
        df_m.groupby(["state", "cy"])[["sales", "txns"]]
            .cumsum()
            .rename(columns={"sales": "sales_curr_ytd", "txns": "txns_curr_ytd"})
    )

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
    if rule == "calendar_prev_curr":
        sales_base = df_v[["sales_prev_yr", "sales_curr_ytd"]].max(axis=1)
        txns_base  = df_v[["txns_prev_yr",  "txns_curr_ytd"] ].max(axis=1)
    elif rule == "calendar_prev":
        sales_base = df_v["sales_prev_yr"]
        txns_base  = df_v["txns_prev_yr"]
    else:  # default -> rolling_12m
        sales_base = df_v["sales_12m"]
        txns_base  = df_v["txns_12m"]

    return pd.DataFrame(
        {
            "sales_met": (sales_base >= sales_th) if sales_th is not None else False,
            "txn_met":   (txns_base  >= txn_th)  if txn_th  is not None else False,
        },
        index=df_v.index,
    )
