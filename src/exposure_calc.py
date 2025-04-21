# src/exposure_calc.py
import pandas as pd
import logging
from dateutil.relativedelta import relativedelta
from src.utils import ErrorCollector
import numpy as np # For NaN comparison

class ExposureCalculator:
    """Calculates potential tax exposure and estimates VDA savings based on config."""

    def __init__(self, nexus_summary_df: pd.DataFrame, sales_with_exemptions_df: pd.DataFrame,
                 state_config: dict, vda_option: str, error_collector: ErrorCollector):
        """
        Initializes the ExposureCalculator.

        Args:
            nexus_summary_df (pd.DataFrame): Summary of nexus triggers by state/month.
            sales_with_exemptions_df (pd.DataFrame): Sales data with 'is_exempt' column applied.
            state_config (dict): Loaded configuration from state_config.yaml.
            vda_option (str): "Estimate" or "None".
            error_collector (ErrorCollector): Instance for collecting errors/warnings.
        """
        self.nexus_summary = nexus_summary_df.copy()
        self.sales_df = sales_with_exemptions_df.copy()
        self.config = state_config
        self.vda_option = vda_option
        self.error_collector = error_collector

        # Ensure required PeriodIndex types exist after nexus analysis
        if not self.nexus_summary.empty:
            if 'month_year' not in self.nexus_summary or not pd.api.types.is_period_dtype(self.nexus_summary['month_year']):
                 raise TypeError("Nexus summary DataFrame must contain a PeriodIndex column 'month_year'.")
            if 'first_trigger_month' not in self.nexus_summary or not pd.api.types.is_period_dtype(self.nexus_summary['first_trigger_month']):
                 # Handle potential all-NaT case gracefully before checking dtype
                 if not self.nexus_summary['first_trigger_month'].isna().all():
                     raise TypeError("Nexus summary DataFrame must contain a PeriodIndex column 'first_trigger_month'.")

        # Ensure sales data has necessary index/columns
        if not isinstance(self.sales_df.index, pd.DatetimeIndex):
            raise TypeError("Sales DataFrame must have a DatetimeIndex for exposure calculation.")
        if 'month_year' not in self.sales_df or not pd.api.types.is_period_dtype(self.sales_df['month_year']):
            # Add month_year if missing
             self.sales_df['month_year'] = self.sales_df.index.to_period('M')

        logging.info(f"ExposureCalculator initialized. VDA Option: {self.vda_option}")


    def _get_state_params(self, state: str) -> dict:
        """ Safely gets parameters for a state from the config. """
        if state not in self.config:
            msg = f"No configuration found for state: {state}. Cannot calculate exposure."
            self.error_collector.add_warning(msg)
            return None
        return self.config[state]

    def _calculate_interest(self, tax_amount, annual_rate, months_late):
        """ Calculates simple monthly interest. """
        if pd.isna(tax_amount) or pd.isna(annual_rate) or pd.isna(months_late) or annual_rate < 0 or months_late < 0:
            return 0.0 # Return 0 if inputs are invalid
        monthly_rate = annual_rate / 12.0
        # Simple interest: P * r * t (where t is number of periods)
        return tax_amount * monthly_rate * months_late

    def calculate_exposure(self) -> pd.DataFrame:
        """
        Calculates potential tax exposure for triggered states.

        Returns:
            pd.DataFrame: DataFrame with monthly taxable sales and estimated tax per state.
                          Includes VDA estimates if requested.
        """
        logging.info("Calculating potential tax exposure...")
        exposure_data = []

        if self.sales_df.empty:
            logging.warning("Sales DataFrame is empty. No exposure calculated.")
            return pd.DataFrame()
        if self.nexus_summary.empty:
             logging.warning("Nexus summary DataFrame is empty. No exposure calculated.")
             return pd.DataFrame()

        # Filter sales data: non-exempt transactions only
        taxable_sales_df = self.sales_df[~self.sales_df['is_exempt']].copy()
        if taxable_sales_df.empty:
            logging.warning("No taxable (non-exempt) sales found. No exposure calculated.")
            return pd.DataFrame()

        # Get the first trigger month for each state that actually triggered nexus
        # Ensure NaT comparison is handled correctly
        first_triggers = self.nexus_summary.loc[self.nexus_summary['first_trigger_month'].notna(),
                                                ['state', 'first_trigger_month']].drop_duplicates()
        # Handle cases where a state might appear multiple times if logic allowed, take the earliest
        first_triggers = first_triggers.loc[first_triggers.groupby('state')['first_trigger_month'].idxmin()].set_index('state')


        if first_triggers.empty:
            logging.info("No states triggered nexus based on the analysis summary. No exposure calculated.")
            return pd.DataFrame()

        # Aggregate taxable sales by state and month
        monthly_taxable_sales = taxable_sales_df.groupby(['state', 'month_year'])['total_amount'].sum().reset_index()
        monthly_taxable_sales.rename(columns={'total_amount': 'taxable_sales'}, inplace=True)

        if monthly_taxable_sales.empty:
             logging.warning("Aggregation of taxable sales resulted in empty DataFrame. No exposure calculated.")
             return pd.DataFrame()

        states_with_exposure_calc = set()
        last_data_month = monthly_taxable_sales['month_year'].max() # Needed for interest/VDA calc

        for state, trigger_info in first_triggers.iterrows():
            state_params = self._get_state_params(state)
            if not state_params: continue

            tax_rate = state_params.get('tax_rate')
            # Use np.isnan for checking potential NaN from YAML if loaded incorrectly, or just check for None
            if tax_rate is None or pd.isna(tax_rate):
                msg = f"Missing or invalid tax_rate for state {state} in config. Cannot calculate exposure for this state."
                self.error_collector.add_warning(msg)
                continue

            # Ensure tax rate is float
            try:
                tax_rate = float(tax_rate)
                if tax_rate < 0: raise ValueError("Tax rate cannot be negative")
            except (ValueError, TypeError):
                 msg = f"Invalid tax_rate format for state {state} in config: '{tax_rate}'. Must be a number. Cannot calculate exposure."
                 self.error_collector.add_warning(msg)
                 continue

            first_trigger_month = trigger_info['first_trigger_month']
            # Exposure typically starts the month *after* the trigger month
            # Some states might have grace periods - TODO: Add grace period logic via quirk_flags if needed
            exposure_start_month = first_trigger_month + 1

            # Filter the aggregated monthly sales for the exposure period for this state
            state_exposure_monthly = monthly_taxable_sales[
                (monthly_taxable_sales['state'] == state) &
                (monthly_taxable_sales['month_year'] >= exposure_start_month)
            ].copy()

            if not state_exposure_monthly.empty:
                states_with_exposure_calc.add(state)
                state_exposure_monthly['tax_rate'] = tax_rate
                state_exposure_monthly['estimated_tax'] = state_exposure_monthly['taxable_sales'] * tax_rate

                # Add VDA calculations if requested
                if self.vda_option == "Estimate":
                    # Get VDA parameters safely
                    vda_lookback_months = state_params.get('vda_lookback_cap', 36)
                    # TODO: Implement lookup/handling for 'statutory' interest rates
                    interest_rate_annual = state_params.get('vda_interest_rate', 0.05) # Placeholder annual rate
                    penalty_rate = state_params.get('standard_penalty_rate', 0.25)
                    vda_penalty_waived = state_params.get('vda_penalty_waived', True)

                    # Validate params
                    try:
                         vda_lookback_months = int(vda_lookback_months) if vda_lookback_months is not None else 36
                         interest_rate_annual = float(interest_rate_annual) if interest_rate_annual is not None else 0.05
                         penalty_rate = float(penalty_rate) if penalty_rate is not None else 0.25
                         if any(p < 0 for p in [vda_lookback_months, interest_rate_annual, penalty_rate]):
                             raise ValueError("VDA parameters (lookback, rates) cannot be negative.")
                    except (ValueError, TypeError) as param_err:
                         msg = f"Invalid VDA parameters for state {state} in config: {param_err}. Skipping VDA estimation for this state."
                         self.error_collector.add_warning(msg)
                         # Add empty columns to maintain schema consistency
                         for col in ['full_interest', 'full_penalty', 'full_liability', 'vda_tax', 'vda_interest', 'vda_penalty', 'vda_liability', 'estimated_vda_savings']:
                              state_exposure_monthly[col] = 0.0
                         exposure_data.append(state_exposure_monthly)
                         continue # Skip to next state


                    # Determine VDA lookback window start month
                    vda_start_month = last_data_month - relativedelta(months=vda_lookback_months - 1)

                    # Calculate months late for interest calc (relative to last data month)
                    # Ensure month_year is PeriodIndex before dt accessor
                    if not pd.api.types.is_period_dtype(state_exposure_monthly['month_year']):
                        state_exposure_monthly['month_year'] = pd.PeriodIndex(state_exposure_monthly['month_year'], freq='M')

                    # Calculate months ago accurately using period arithmetic
                    state_exposure_monthly['months_late'] = (last_data_month.year - state_exposure_monthly['month_year'].apply(lambda p: p.year)) * 12 + \
                                                            (last_data_month.month - state_exposure_monthly['month_year'].apply(lambda p: p.month))


                    # Full Liability (Audit Scenario) - calculated for all months in exposure period
                    state_exposure_monthly['full_interest'] = state_exposure_monthly.apply(
                        lambda row: self._calculate_interest(row['estimated_tax'], interest_rate_annual, row['months_late']), axis=1
                    )
                    state_exposure_monthly['full_penalty'] = state_exposure_monthly['estimated_tax'] * penalty_rate
                    state_exposure_monthly['full_liability'] = state_exposure_monthly['estimated_tax'] + state_exposure_monthly['full_interest'] + state_exposure_monthly['full_penalty']

                    # VDA Liability - calculated only for months within VDA lookback window
                    # Tax within VDA lookback
                    state_exposure_monthly['vda_tax'] = state_exposure_monthly['estimated_tax'].where(state_exposure_monthly['month_year'] >= vda_start_month, 0)
                    # Interest on VDA tax for relevant months
                    state_exposure_monthly['vda_interest'] = state_exposure_monthly.apply(
                         lambda row: self._calculate_interest(row['vda_tax'], interest_rate_annual, row['months_late']) if row['month_year'] >= vda_start_month else 0, axis=1
                    )
                     # Penalty on VDA tax (often waived)
                    state_exposure_monthly['vda_penalty'] = state_exposure_monthly['vda_tax'] * penalty_rate * (0 if vda_penalty_waived else 1)
                    state_exposure_monthly['vda_liability'] = state_exposure_monthly['vda_tax'] + state_exposure_monthly['vda_interest'] + state_exposure_monthly['vda_penalty']

                    # Estimated Savings
                    state_exposure_monthly['estimated_vda_savings'] = state_exposure_monthly['full_liability'] - state_exposure_monthly['vda_liability']

                    # Drop helper column
                    state_exposure_monthly = state_exposure_monthly.drop(columns=['months_late'])

                exposure_data.append(state_exposure_monthly)

        if not exposure_data:
            logging.warning("No exposure calculated after processing all triggered states (likely no sales in post-trigger periods).")
            return pd.DataFrame()

        exposure_df = pd.concat(exposure_data, ignore_index=True)

        # Ensure all potential VDA columns exist even if VDA wasn't run for all states
        vda_cols = ['full_interest', 'full_penalty', 'full_liability', 'vda_tax', 'vda_interest', 'vda_penalty', 'vda_liability', 'estimated_vda_savings']
        for col in vda_cols:
            if col not in exposure_df.columns:
                exposure_df[col] = 0.0 # Or np.nan if preferred

        num_states_exp = len(states_with_exposure_calc)
        self.error_collector.update_summary('states_with_exposure', num_states_exp)
        logging.info(f"Potential tax exposure calculated for {num_states_exp} state(s).")
        return exposure_df