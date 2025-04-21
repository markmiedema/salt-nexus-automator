# src/nexus_analysis.py
import pandas as pd
import logging
from dateutil.relativedelta import relativedelta
from src.utils import ErrorCollector

class NexusAnalyzer:
    """Performs state economic nexus analysis based on configured thresholds and rules."""

    def __init__(self, standardized_df: pd.DataFrame, state_config: dict, error_collector: ErrorCollector):
        """
        Initializes the NexusAnalyzer.

        Args:
            standardized_df (pd.DataFrame): Cleaned and standardized sales data with datetime index.
            state_config (dict): Loaded configuration from state_config.yaml.
            error_collector (ErrorCollector): Instance for collecting errors/warnings.
        """
        self.df = standardized_df.copy()
        self.config = state_config
        self.error_collector = error_collector

        # Ensure 'date' is datetime index (should be done post-standardization)
        if not isinstance(self.df.index, pd.DatetimeIndex):
             msg="Input DataFrame must have a DatetimeIndex named 'date'."
             logging.error(msg)
             # Handle error appropriately - maybe raise or return empty results?
             raise ValueError(msg)

        self.df.sort_index(inplace=True)
        # Add month_year period for efficient grouping/joining later
        self.df['month_year'] = self.df.index.to_period('M')
        logging.info(f"NexusAnalyzer initialized. Data spans from {self.df.index.min()} to {self.df.index.max()}.")


    def _get_state_params(self, state: str) -> dict:
        """ Safely gets parameters for a state from the config. """
        if state not in self.config:
            # Only warn once per state
            msg = f"No configuration found for state: {state}. Skipping analysis for this state."
            self.error_collector.add_warning(msg) # add_warning handles duplicates
            return None
        # Return a copy to prevent modification? Or assume read-only.
        return self.config[state]

    def _calculate_rolling_aggregates(self, state_df: pd.DataFrame, rule: str, details: dict, current_period: pd.Period) -> pd.Series:
        """
        Calculates sales and transactions based on lookback rule defined in config.

        Args:
            state_df (pd.DataFrame): DataFrame filtered for the specific state and relevant sales channels.
            rule (str): The lookback rule string (e.g., 'rolling_12m', 'calendar_prev_curr').
            details (dict): The lookback_details dictionary from config.
            current_period (pd.Period): The month/period for which to calculate the lookback.

        Returns:
            pd.Series: Containing 'rolling_sales' and 'rolling_transactions'.
        """
        sales = 0.0
        transactions = 0

        # Ensure data has expected columns and index before calculations
        if state_df.empty or 'total_amount' not in state_df.columns or 'invoice_number' not in state_df.columns:
            return pd.Series({'rolling_sales': 0.0, 'rolling_transactions': 0})

        try:
            if rule == 'rolling_12m':
                months = details.get('months', 12)
                # Window ends at the end of the current period
                end_date = current_period.end_time
                # Window starts N-1 months before the current period's start
                start_date = (current_period - (months - 1)).start_time
                window_df = state_df[(state_df.index >= start_date) & (state_df.index <= end_date)]
                sales = window_df['total_amount'].sum()
                # Count distinct non-zero transactions within the window
                transactions = window_df[window_df['total_amount'] != 0]['invoice_number'].nunique()

            elif rule == 'calendar_prev_curr':
                # Based on *EITHER* the complete previous calendar year *OR* the current calendar year-to-date
                current_year = current_period.year
                prev_year = current_year - 1

                # Previous full calendar year
                start_prev_yr = pd.Timestamp(f'{prev_year}-01-01')
                end_prev_yr = pd.Timestamp(f'{prev_year}-12-31')
                prev_yr_df = state_df[(state_df.index >= start_prev_yr) & (state_df.index <= end_prev_yr)]
                sales_prev_yr = prev_yr_df['total_amount'].sum()
                tx_prev_yr = prev_yr_df[prev_yr_df['total_amount'] != 0]['invoice_number'].nunique()

                # Current calendar year up to end of current period
                start_curr_yr = pd.Timestamp(f'{current_year}-01-01')
                end_curr_period = current_period.end_time
                curr_ytd_df = state_df[(state_df.index >= start_curr_yr) & (state_df.index <= end_curr_period)]
                sales_curr_ytd = curr_ytd_df['total_amount'].sum()
                tx_curr_ytd = curr_ytd_df[curr_ytd_df['total_amount'] != 0]['invoice_number'].nunique()

                # Use the max value from either period for the threshold check for this month
                sales = max(sales_prev_yr, sales_curr_ytd)
                transactions = max(tx_prev_yr, tx_curr_ytd)

            elif rule == 'calendar_prev':
                 # Based *ONLY* on the complete previous calendar year
                 prev_year = current_period.year - 1
                 start_prev_yr = pd.Timestamp(f'{prev_year}-01-01')
                 end_prev_yr = pd.Timestamp(f'{prev_year}-12-31')
                 prev_yr_df = state_df[(state_df.index >= start_prev_yr) & (state_df.index <= end_prev_yr)]
                 sales = prev_yr_df['total_amount'].sum()
                 transactions = prev_yr_df[prev_yr_df['total_amount'] != 0]['invoice_number'].nunique()

            elif rule == 'rolling_4q':
                # Preceding 4 *completed* quarters ending prior to the *start* of the current quarter might be typical interpretation.
                # OR interpretation: 12 months ending at end of *prior* quarter. Let's use simpler rolling 12 for now.
                # TODO: Clarify exact "rolling 4 quarter" definition if needed for states like NY/IL/VT.
                # Using rolling_12m as a proxy for now, adjust if needed.
                logging.debug(f"Using 'rolling_12m' logic as proxy for 'rolling_4q' rule for period {current_period}. Verify specific state definition.")
                months = 12 # Standard 12 months for proxy
                end_date = current_period.end_time
                start_date = (current_period - (months - 1)).start_time
                window_df = state_df[(state_df.index >= start_date) & (state_df.index <= end_date)]
                sales = window_df['total_amount'].sum()
                transactions = window_df[window_df['total_amount'] != 0]['invoice_number'].nunique()

            elif rule == 'accounting_year':
                 # Needs definition of 'accounting_year' - fiscal year end? Assume calendar for now.
                 # TODO: Implement logic based on actual definition of 'accounting_year' (e.g., for PR)
                 logging.warning(f"Lookback rule 'accounting_year' not fully implemented. Using 'calendar_prev' logic as fallback for period {current_period}.")
                 prev_year = current_period.year - 1
                 start_prev_yr = pd.Timestamp(f'{prev_year}-01-01')
                 end_prev_yr = pd.Timestamp(f'{prev_year}-12-31')
                 prev_yr_df = state_df[(state_df.index >= start_prev_yr) & (state_df.index <= end_prev_yr)]
                 sales = prev_yr_df['total_amount'].sum()
                 transactions = prev_yr_df[prev_yr_df['total_amount'] != 0]['invoice_number'].nunique()


            elif rule == 'none':
                # For states with no sales tax (DE, MT, NH, OR)
                sales = 0.0
                transactions = 0

            else:
                msg = f"Unsupported lookback rule '{rule}' found in config. Calculation defaulted to 0."
                self.error_collector.add_warning(msg)
                sales = 0.0
                transactions = 0

        except Exception as e:
             logging.error(f"Error during rolling aggregate calculation for rule '{rule}', period {current_period}: {e}", exc_info=True)
             return pd.Series({'rolling_sales': 0.0, 'rolling_transactions': 0}) # Return zeros on error

        return pd.Series({'rolling_sales': sales, 'rolling_transactions': transactions})


    def analyze_nexus(self) -> pd.DataFrame:
        """
        Analyzes nexus for all states based on configured thresholds and lookback periods.

        Returns:
            pd.DataFrame: A DataFrame summarizing nexus trigger status per state per month.
                          Includes flags for meeting sales/txn thresholds and first trigger month.
        """
        results = []
        all_states = sorted(self.df['state'].unique()) # Process states alphabetically

        if self.df.empty:
             logging.warning("Standardized DataFrame is empty. Cannot perform nexus analysis.")
             return pd.DataFrame()

        min_month = self.df['month_year'].min()
        max_month = self.df['month_year'].max()
        # Create a full range of periods to analyze consistently across states
        analysis_periods = pd.period_range(start=min_month, end=max_month, freq='M')

        logging.info(f"Starting nexus analysis for {len(all_states)} states from {min_month} to {max_month}...")

        # Pre-filter marketplace sales if needed *once*
        # TODO: Make marketplace channel identifiers configurable
        marketplace_channels = ['AMAZON-FBA', 'ETSY', 'EBAY'] # Example list
        is_marketplace_sale = self.df['sales_channel'].str.upper().isin(marketplace_channels)
        df_non_marketplace = self.df[~is_marketplace_sale]

        for state in all_states:
            state_params = self._get_state_params(state)
            if not state_params: continue # Skips state if no config

            # Get state specific parameters
            sales_thresh = state_params.get('sales_threshold')
            txn_thresh = state_params.get('transaction_threshold')
            lookback_rule = state_params.get('lookback_rule', 'rolling_12m')
            lookback_details = state_params.get('lookback_details', {})
            include_marketplace = state_params.get('marketplace_threshold_inclusion', True)

            # Skip states with no thresholds defined or no applicable rule
            if (sales_thresh is None and txn_thresh is None) or lookback_rule == 'none':
                logging.info(f"Skipping nexus analysis for state {state} (no thresholds or rule is 'none').")
                continue

            # Select the appropriate dataframe for threshold calculation
            if include_marketplace:
                state_df_for_thresholds = self.df[self.df['state'] == state]
            else:
                state_df_for_thresholds = df_non_marketplace[df_non_marketplace['state'] == state]
                logging.debug(f"{state}: Using non-marketplace sales for threshold calculations.")

            if state_df_for_thresholds.empty:
                logging.debug(f"No relevant sales data found for state {state} for threshold calculation. Skipping detailed analysis.")
                # Optionally add entries with 0 results if needed for reporting consistency
                continue

            state_results = []
            first_trigger_month_for_state = pd.NaT # Track first trigger for *this* state

            for period in analysis_periods:
                 # Calculate aggregates based on the lookback rule for the current period
                 agg_results = self._calculate_rolling_aggregates(
                     state_df_for_thresholds, # Use potentially filtered data
                     lookback_rule,
                     lookback_details,
                     period
                 )

                 # --- Core Threshold Logic ---
                 # Check if sales threshold exists AND is met
                 sales_met = (agg_results['rolling_sales'] >= sales_thresh) if sales_thresh is not None else False

                 # Check if transaction threshold exists AND is met
                 txn_met = (agg_results['rolling_transactions'] >= txn_thresh) if txn_thresh is not None else False

                 # Nexus is triggered if EITHER defined threshold condition is met
                 nexus_triggered = sales_met or txn_met
                 # --- End Core Threshold Logic ---

                 # Track the first month nexus was triggered for this state
                 if nexus_triggered and pd.isna(first_trigger_month_for_state):
                     first_trigger_month_for_state = period
                     logging.info(f"Nexus triggered for state {state} in period {period}. Sales: {agg_results['rolling_sales']:.2f}, Txns: {agg_results['rolling_transactions']}.")


                 state_results.append({
                     'state': state,
                     'month_year': period,
                     'rolling_sales': agg_results['rolling_sales'],
                     'rolling_transactions': agg_results['rolling_transactions'],
                     'sales_threshold': sales_thresh,
                     'txn_threshold': txn_thresh,
                     'lookback_rule': lookback_rule,
                     'sales_met': sales_met,
                     'txn_met': txn_met,
                     'nexus_triggered': nexus_triggered,
                     # Use the consistent value once trigger found
                     'first_trigger_month': first_trigger_month_for_state
                 })

            if state_results:
                results.extend(state_results)

        if not results:
            logging.warning("No nexus analysis results generated after processing all states.")
            return pd.DataFrame()

        # Combine results into a single DataFrame
        summary_df = pd.DataFrame(results)

        # Ensure correct dtypes for period columns
        summary_df['month_year'] = pd.PeriodIndex(summary_df['month_year'], freq='M')
        summary_df['first_trigger_month'] = pd.PeriodIndex(summary_df['first_trigger_month'].astype(str), freq='M') # Handle potential NaT conversion issues


        # Update summary statistics
        triggered_states_count = summary_df.loc[summary_df['first_trigger_month'].notna(), 'state'].nunique()
        self.error_collector.update_summary('nexus_triggers', triggered_states_count)
        logging.info(f"Nexus analysis complete. Found potential first triggers in {triggered_states_count} state(s).")

        # Define final column order
        cols_order = [
            'state', 'month_year', 'rolling_sales', 'rolling_transactions',
            'sales_threshold', 'txn_threshold', 'sales_met', 'txn_met',
            'nexus_triggered', 'first_trigger_month', 'lookback_rule'
        ]
        # Ensure all columns exist, add missing ones as NaN if needed
        for col in cols_order:
            if col not in summary_df.columns:
                summary_df[col] = None
        summary_df = summary_df[cols_order]

        return summary_df