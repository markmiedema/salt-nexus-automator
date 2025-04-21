# src/nexus_analysis.py
"""Economic‑Nexus analysis engine (vectorised edition).

This refactor eliminates Python‑level nested loops by leaning on the
helper functions in `src.agg_utils`.  It turns raw transaction data into
monthly metrics, evaluates each state’s thresholds, and returns a tidy
DataFrame with a `first_trigger_month` column.
"""

from __future__ import annotations

import logging
from typing import Dict, Any

import pandas as pd

from src.agg_utils import (
    monthly_state_summary,
    add_rolling_12m,
    add_calendar_year_metrics,
    evaluate_thresholds,
    MARKETPLACE_CHANNELS,  # <- parsed once inside agg_utils
)

logger = logging.getLogger(__name__)


class NexusAnalyzer:
    """Core façade class.  Feed it a state‑config dict, get triggers back."""

    # --------------------------------------------------------------------- #
    # Constructor                                                           #
    # --------------------------------------------------------------------- #
    def __init__(self, state_config: Dict[str, Dict[str, Any]]):
        """
        Parameters
        ----------
        state_config : dict
            Mapping ``'AL'`` → ``{'lookback_rule': 'rolling_12m',
                                  'sales_threshold': 250_000,
                                  'transaction_threshold': None}``
        """
        self.state_config = state_config

    # --------------------------------------------------------------------- #
    # Main API                                                              #
    # --------------------------------------------------------------------- #
    def analyze_nexus(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Vectorised economic‑nexus analysis.

        Expected columns in ``df_raw``
        --------------------------------
        * ``invoice_date``  – parseable date
        * ``invoice_number`` – unique per transaction
        * ``state``         – two‑letter abbreviation
        * ``total_amount``  – numeric
        * ``channel``       – (OPTIONAL) marketplace vs DTC

        Returns
        -------
        DataFrame
            Monthly metrics + ``sales_met``, ``txn_met`` flags and
            ``first_trigger_month`` for each state.
        """

        # --- Pre‑processing ---------------------------------------------
        df = df_raw.copy()

        # Ensure we have a period column
        if "month_year" not in df.columns:
            df["month_year"] = pd.to_datetime(df["invoice_date"]).dt.to_period("M")

        # Filter marketplace channels *if* your rules exclude them.
        if "channel" in df.columns:
            df = df.loc[~df["channel"].str.upper().isin(MARKETPLACE_CHANNELS)]

        # --- 1. Monthly aggregation -------------------------------------
        df_m = monthly_state_summary(df)  # (state, month) index

        # --- 2. Rolling 12‑month totals ---------------------------------
        df_m = add_rolling_12m(df_m)

        # --- 3. Calendar‑year helpers -----------------------------------
        df_m = add_calendar_year_metrics(df_m)

        # --- 4. Threshold evaluation per state --------------------------
        results = []
        for state in df_m.index.get_level_values("state").unique():
            slice_ = df_m.loc[state].copy()

            cfg = self.state_config.get(state)
            if cfg is None:
                logger.warning("No config for %s; skipping", state)
                continue

            slice_ = evaluate_thresholds(
                slice_,
                cfg.get("lookback_rule", "rolling_12m"),
                cfg.get("sales_threshold"),
                cfg.get("transaction_threshold"),
            )
            slice_["state"] = state  # re‑add for merge later
            results.append(slice_)

        if not results:
            raise ValueError("No states processed—check inputs & config.")

        df_out = pd.concat(results).reset_index()

        # --- 5. First trigger month -------------------------------------
        first_hit = (
            df_out.loc[df_out["sales_met"] | df_out["txn_met"]]
            .groupby("state")["month_year"]
            .min()
            .rename("first_trigger_month")
        )
        df_out = df_out.merge(first_hit, on="state", how="left")

        return df_out


__all__ = ["NexusAnalyzer"]
