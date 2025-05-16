# src/salt_nexus_automator/exposure_calc.py
"""Compute potential sales-tax exposure and (optionally) VDA savings."""

from __future__ import annotations

import logging
from typing import Dict, Any

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd

from .utils import ErrorCollector


class ExposureCalculator:
    """Calculates potential tax exposure and estimates VDA savings."""

    # ─────────────────────────────────────────────────────────────
    # 1. constructor (lightweight)
    # ─────────────────────────────────────────────────────────────
    def __init__(
        self,
        state_config: Dict[str, Dict[str, Any]],
        vda_option: str = "Estimate",
        error_collector: ErrorCollector | None = None,
    ) -> None:
        """
        Parameters
        ----------
        state_config
            Parsed configuration from ``state_config.yaml`` (one dict per state).
        vda_option
            ``"Estimate"`` (default) or ``"None"`` – whether to run VDA logic.
        error_collector
            Optional collector for warnings / validation errors.
        """
        self.config = state_config
        self.vda_option = vda_option
        self.error_collector = error_collector

        # will be set on first calculate_exposure() call
        self.sales_df: pd.DataFrame | None = None
        self.nexus_summary: pd.DataFrame | None = None

        logging.info("ExposureCalculator initialised (VDA option=%s).", vda_option)

    # ─────────────────────────────────────────────────────────────
    # 2. helper methods
    # ─────────────────────────────────────────────────────────────
    def _get_state_params(self, state: str) -> Dict[str, Any] | None:
        params = self.config.get(state)
        if params is None and self.error_collector:
            self.error_collector.add_warning(
                f"No configuration found for state '{state}'. Skipping."
            )
        return params

    @staticmethod
    def _calculate_interest(
        tax_amount: float, annual_rate: float, months_late: int
    ) -> float:
        """Simple monthly-interest calculator with guards."""
        if (
            pd.isna(tax_amount)
            or pd.isna(annual_rate)
            or pd.isna(months_late)
            or annual_rate < 0
            or months_late < 0
        ):
            return 0.0
        return tax_amount * (annual_rate / 12.0) * months_late

    # ─────────────────────────────────────────────────────────────
    # 3. public API
    # ─────────────────────────────────────────────────────────────
    def calculate_exposure(
        self,
        sales_df: pd.DataFrame,
        nexus_summary: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Compute exposure for every state that triggered nexus.

        Parameters
        ----------
        sales_df
            Standardised row-level sales data.
        nexus_summary
            Output of ``NexusAnalyzer`` with ``first_trigger_month`` per state.

        Returns
        -------
        pd.DataFrame
            Exposure detail by state / month, or empty frame if nothing to do.
        """
        # ── early exits ──────────────────────────────────────────
        if sales_df.empty:
            logging.warning("Sales DataFrame is empty. No exposure calculated.")
            return pd.DataFrame()
        if nexus_summary.empty:
            logging.warning("Nexus summary is empty. No exposure calculated.")
            return pd.DataFrame()

        # ── normalise sales index / month_year ──────────────────
        # Ensure sales index is DatetimeIndex
        if not isinstance(sales_df.index, pd.DatetimeIndex):
            if "invoice_date" in sales_df.columns:
                # Coerce 'invoice_date' to datetime and set as index
                # Use .copy() to avoid SettingWithCopyWarning if sales_df is a slice
                sales_df = sales_df.copy()
                sales_df["invoice_date"] = pd.to_datetime(sales_df["invoice_date"])
                sales_df = sales_df.set_index("invoice_date")
            else:
                raise TypeError(
                    "Sales DataFrame must contain 'invoice_date' column if index is not a DatetimeIndex."
                )

        # Ensure 'month_year' column exists and is Period[M] based on the (now confirmed) DatetimeIndex
        # This handles cases where month_year might be missing or not the correct dtype
        if "month_year" not in sales_df.columns or not pd.api.types.is_period_dtype(
            sales_df["month_year"]
        ):
             # Create or overwrite 'month_year' from the DatetimeIndex
            sales_df["month_year"] = sales_df.index.to_period("M")


        # ── validate / auto-coerce nexus_summary columns ────────
        # auto-convert first_trigger_month to Period[M] if needed
        if (
            "first_trigger_month" in nexus_summary.columns
            and not pd.api.types.is_period_dtype(nexus_summary["first_trigger_month"])
        ):
            logging.info("Coercing 'first_trigger_month' to Period[M] in nexus_summary.")
            nexus_summary["first_trigger_month"] = pd.PeriodIndex(
                pd.to_datetime(nexus_summary["first_trigger_month"]), freq="M"
            )

        # Validate required Period columns (now only first_trigger_month)
        for col in ("first_trigger_month",):
            if col not in nexus_summary.columns or not pd.api.types.is_period_dtype(
                nexus_summary[col]
            ):
                raise TypeError(
                    f"Nexus summary must contain PeriodIndex column '{col}'."
                )

        # store for possible downstream use
        self.sales_df = sales_df
        self.nexus_summary = nexus_summary

        logging.info("Calculating potential tax exposure...")
        exposure_chunks: list[pd.DataFrame] = []
        states_with_exposure: set[str] = set()

        # filter taxable rows once
        taxable_df = sales_df[~sales_df["is_exempt"]].copy()
        if taxable_df.empty:
            logging.warning("No taxable (non-exempt) sales found.")
            return pd.DataFrame()

        # first trigger per state
        first_triggers = (
            nexus_summary.loc[nexus_summary["first_trigger_month"].notna(), ["state", "first_trigger_month"]]
            .drop_duplicates()
            .loc[lambda d: d.groupby("state")["first_trigger_month"].idxmin()]
            .set_index("state")
        )
        if first_triggers.empty:
            logging.info("No states triggered nexus according to summary.")
            return pd.DataFrame()

        # aggregate taxable sales by state × month
        monthly_taxable = (
            taxable_df
            .groupby(["state", "month_year"])["total_amount"]
            .sum()
            .reset_index()
            .rename(columns={"total_amount": "taxable_sales"})
        )
        if monthly_taxable.empty:
            logging.warning("Monthly aggregation produced empty DataFrame.")
            return pd.DataFrame()

        last_month: pd.Period = monthly_taxable["month_year"].max()

        # ── iterate states ──────────────────────────────────────
        for state, trig_row in first_triggers.iterrows():
            params = self._get_state_params(state)
            if params is None:
                continue

            try:
                tax_rate = float(params.get("tax_rate"))
                if tax_rate < 0:
                    raise ValueError
            except (TypeError, ValueError):
                if self.error_collector:
                    self.error_collector.add_warning(
                        f"Invalid or missing tax_rate for {state}. Skipping."
                    )
                continue

            # Start calculating exposure from the month *after* the trigger month
            start_month = trig_row["first_trigger_month"] + 1
            state_exposure = monthly_taxable[
                (monthly_taxable["state"] == state)
                & (monthly_taxable["month_year"] >= start_month)
            ].copy()

            if state_exposure.empty:
                logging.info(f"No taxable sales found for {state} after trigger month.")
                continue

            states_with_exposure.add(state)
            state_exposure["tax_rate"] = tax_rate
            state_exposure["estimated_tax"] = state_exposure["taxable_sales"] * tax_rate

            # ── VDA estimation ───────────────────────────────────
            if self.vda_option == "Estimate":
                # Get VDA parameters from config, with sensible defaults
                vda_lookback = int(params.get("vda_lookback_cap", 36) or 36)
                int_rate = float(params.get("vda_interest_rate", 0.05) or 0.05)
                p_rate = float(params.get("standard_penalty_rate", 0.25) or 0.25)
                penalty_waived = bool(params.get("vda_penalty_waived", True))

                # Calculate the start month for VDA based on lookback from the last month in data
                # Corrected: Subtract integer periods from Period object
                vda_start = last_month - (vda_lookback - 1)


                # Ensure month_year is Period dtype for calculations
                if not pd.api.types.is_period_dtype(state_exposure["month_year"]):
                    state_exposure["month_year"] = pd.PeriodIndex(
                        state_exposure["month_year"], freq="M"
                    )

                # Calculate months late relative to the last month in the data
                state_exposure["months_late"] = (
                    (last_month.year - state_exposure["month_year"].dt.year) * 12
                    + (last_month.month - state_exposure["month_year"].dt.month)
                )

                # Calculate full interest and penalty
                state_exposure["full_interest"] = state_exposure.apply(
                    lambda r: self._calculate_interest(r["estimated_tax"], int_rate, r["months_late"]),
                    axis=1,
                )
                state_exposure["full_penalty"] = state_exposure["estimated_tax"] * p_rate
                state_exposure["full_liability"] = (
                    state_exposure["estimated_tax"]
                    + state_exposure["full_interest"]
                    + state_exposure["full_penalty"]
                )

                # Calculate VDA specific amounts (tax, interest, penalty)
                state_exposure["vda_tax"] = state_exposure["estimated_tax"].where(
                    state_exposure["month_year"] >= vda_start, 0
                )
                # Interest is only calculated for months within the VDA lookback period
                state_exposure["vda_interest"] = state_exposure.apply(
                    lambda r: self._calculate_interest(
                        r["vda_tax"], int_rate, r["months_late"]
                    )
                    if r["month_year"] >= vda_start # Apply interest only if within VDA period
                    else 0,
                    axis=1,
                )
                # Penalty may be waived based on config
                state_exposure["vda_penalty"] = (
                    state_exposure["vda_tax"] * p_rate * (0 if penalty_waived else 1)
                )
                state_exposure["vda_liability"] = (
                    state_exposure["vda_tax"]
                    + state_exposure["vda_interest"]
                    + state_exposure["vda_penalty"]
                )
                state_exposure["estimated_vda_savings"] = (
                    state_exposure["full_liability"] - state_exposure["vda_liability"]
                )

                # Drop the temporary 'months_late' column
                state_exposure.drop(columns="months_late", inplace=True)

            exposure_chunks.append(state_exposure)

        # ── consolidate ─────────────────────────────────────────
        if not exposure_chunks:
            logging.warning("No exposure calculated after processing triggered states.")
            return pd.DataFrame()

        exposure_df = pd.concat(exposure_chunks, ignore_index=True)

        # guarantee VDA columns exist, even if VDA wasn't estimated (fill with 0.0)
        for col in (
            "full_interest",
            "full_penalty",
            "full_liability",
            "vda_tax",
            "vda_interest",
            "vda_penalty",
            "vda_liability",
            "estimated_vda_savings",
        ):
            if col not in exposure_df.columns:
                exposure_df[col] = 0.0

        if self.error_collector:
            self.error_collector.update_summary(
                "states_with_exposure", len(states_with_exposure)
            )

        logging.info(
            "Potential tax exposure calculated for %s state(s).",
            len(states_with_exposure),
        )
        return exposure_df
