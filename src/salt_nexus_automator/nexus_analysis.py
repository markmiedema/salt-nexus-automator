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

# Changed import from absolute (src.agg_utils) to relative (.agg_utils)
from .agg_utils import (
    monthly_state_summary,
    add_rolling_12m,
    add_calendar_year_metrics,
    evaluate_thresholds,
    MARKETPLACE_CHANNELS,  # parsed once inside agg_utils
)

logger = logging.getLogger(__name__)


class NexusAnalyzer:
    """Core façade class.  Feed it a state‑config dict, get triggers back."""

    # --------------------------------------------------------------------- #
    # Constructor                                                         #
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
    # Main API                                                            #
    # --------------------------------------------------------------------- #
    def analyze_nexus(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Vectorised economic‑nexus analysis.

        Expected columns in ``df_raw``
        --------------------------------
        * ``invoice_date``  – parseable date
        * ``invoice_number`` – unique per transaction
        * ``state``         – two‑letter abbreviation
        * ``total_amount``  – numeric
        * ``channel``       – (OPTIONAL) marketplace vs DTC

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
            # Attempt to create month_year from invoice_date if it exists
            if "invoice_date" in df.columns:
                 # Ensure invoice_date is datetime first
                 df["invoice_date"] = pd.to_datetime(df["invoice_date"])
                 df["month_year"] = df["invoice_date"].dt.to_period("M")
            else:
                 # If month_year is missing and no invoice_date, raise error
                 raise ValueError("Input DataFrame must contain 'month_year' or 'invoice_date' column.")


        # Filter marketplace channels *if* your rules exclude them.
        # This filtering should happen *before* aggregation.
        if "channel" in df.columns:
            df = df.loc[~df["channel"].str.upper().isin(MARKETPLACE_CHANNELS)]

        # --- 1. Monthly aggregation -------------------------------------
        # This function returns a DataFrame with a MultiIndex ['state', 'month_year']
        df_m = monthly_state_summary(df)

        # --- 2. Rolling 12‑month totals ---------------------------------
        # This function expects a DataFrame with a MultiIndex ['state', 'month_year']
        df_m = add_rolling_12m(df_m)

        # --- 3. Calendar‑year helpers -----------------------------------
        # This function expects a DataFrame with a MultiIndex ['state', 'month_year'] and 'cy' column
        df_m = add_calendar_year_metrics(df_m)

        # --- 4. Threshold evaluation per state --------------------------
        results = []
        # Iterate through each unique state in the monthly aggregated data
        for state in df_m.index.get_level_values("state").unique():
            # Select the data slice for the current state
            slice_ = df_m.loc[state].copy()

            # Get the configuration for the current state
            cfg = self.state_config.get(state)
            if cfg is None:
                logger.warning("No config found for state %s; skipping threshold evaluation.", state)
                continue

            # Evaluate thresholds for the state's data slice
            evaluated_slice = evaluate_thresholds(
                slice_,
                cfg.get("lookback_rule", "rolling_12m"), # Use rule from config or default
                cfg.get("sales_threshold"),
                cfg.get("transaction_threshold"),
            )
            # The evaluate_thresholds function returns a DataFrame with the same index as the input slice
            # Add the state back as a column for concatenation later
            evaluated_slice["state"] = state
            results.append(evaluated_slice)

        # Concatenate the results from all states
        if not results:
            # If no states were processed (e.g., due to missing config), handle this case
            logger.warning("No states were processed for threshold evaluation.")
            # Return an empty DataFrame with expected columns
            return pd.DataFrame(columns=["state", "month_year", "sales_met", "txn_met", "first_trigger_month"])


        # Concatenate results and reset the index to flatten it
        df_out = pd.concat(results).reset_index()

        # --- 5. Determine first trigger month ---------------------------
        # Identify months where either sales or transaction threshold was met
        triggered_months = df_out.loc[df_out["sales_met"] | df_out["txn_met"]]

        # Group by state and find the minimum (earliest) month_year for each state that triggered
        first_hit = (
             triggered_months
            .groupby("state")["month_year"]
            .min()
            .rename("first_trigger_month") # Rename the resulting Series to 'first_trigger_month'
        )

        # Merge the first_trigger_month back into the main output DataFrame
        # Use a left merge to keep all months in df_out, adding first_trigger_month where available
        df_out = df_out.merge(first_hit, on="state", how="left")

        logger.info("Nexus analysis complete.")
        return df_out


__all__ = ["NexusAnalyzer"]
