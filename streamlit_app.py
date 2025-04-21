# streamlit_app.py
import streamlit as st
import pandas as pd
import os
import yaml # For state_config.yaml
import json # For reading summary json
import logging
from datetime import datetime
import time # For performance timing (optional)

# Assuming src modules are importable and updated
from src.utils import ErrorCollector, load_yaml_config, setup_logging
from src.ingestion import DataLoader
from src.standardization import DataStandardizer
from src.nexus_analysis import NexusAnalyzer
from src.exemptions import ExemptionManager
from src.exposure_calc import ExposureCalculator
from src.reporting import ReportGenerator

# --- Page Configuration & Basic Setup ---
st.set_page_config(
    page_title="SALT Nexus Analysis Tool",
    page_icon="📊",
    layout="wide"
)
setup_logging() # Configure logging to stdout

# --- Configuration Paths ---
CONFIG_FILE = 'config/state_config.yaml'
DEFAULT_OUTPUT_DIR = 'output/streamlit_run' # Specific output for streamlit runs

# --- Load Central Config ---
@st.cache_data # Cache the config loading
def get_state_config(config_path):
    try:
        return load_yaml_config(config_path)
    except Exception as e:
        st.error(f"Fatal Error: Could not load state configuration from {config_path}. Please check the file. Error: {e}")
        st.stop() # Halt execution if config fails
state_config = get_state_config(CONFIG_FILE)

# --- Logo & Title ---
logo_path = os.environ.get('REPORT_LOGO_PATH', None)
if logo_path and os.path.exists(logo_path):
    st.image(logo_path, width=200)
else:
    st.write("(Optional: Set REPORT_LOGO_PATH in .env for logo)")

st.title("SALT Economic Nexus Analysis Automation Tool")
st.markdown("Analyze sales data against state-specific economic nexus thresholds.")

# --- Sidebar for Inputs & Controls ---
with st.sidebar:
    st.header("1. Upload Data")
    uploaded_sales_files = st.file_uploader(
        "Upload Sales Files (CSV/Excel)",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True,
        help="Requires columns: date, invoice_number, invoice_date, total_amount, customer_name, street_address, city, state, zip_code, sales_channel"
    )

    uploaded_exemption_file = st.file_uploader(
        "Optional: Upload Exemption List (CSV)",
        type=['csv'],
        accept_multiple_files=False,
        help="CSV with 'customer_id' or 'invoice_number' header for supplemental exemptions."
    )

    st.header("2. Configuration")
    client_name = st.text_input("Client Name", value="Acme Widgets Inc.")
    vda_option = st.selectbox("VDA Calculation Option", ["None", "Estimate"], index=1) # Default to Estimate

    # Add placeholders for future config options if needed

    st.header("3. Run Analysis")
    run_button = st.button("Run Nexus Analysis", type="primary", disabled=(not uploaded_sales_files))

# --- Main Panel for Results ---
if run_button and uploaded_sales_files:
    st.header("📊 Analysis Execution & Results")
    progress_bar = st.progress(0, text="Initializing...")
    status_text = st.empty()
    st.subheader("Execution Log")
    log_box = st.empty() # Placeholder for log messages (optional advanced feature)
    logs = ["Starting analysis..."] # Simple list to store log messages

    # Instantiate Error Collector for this run
    error_collector = ErrorCollector()

    # Define output directory for this run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_output_dir = os.path.join(DEFAULT_OUTPUT_DIR, f"run_{timestamp}")
    os.makedirs(run_output_dir, exist_ok=True)

    try:
        run_start_time = time.time()

        # --- Workflow Steps ---
        # 1. Ingestion
        status_text.text("Step 1/6: Loading data...")
        logs.append("Loading data...")
        log_box.info("\n".join(logs))
        # Save uploaded files temporarily
        temp_sales_paths = []
        temp_exempt_path = None
        temp_dir = f"temp_uploads_{timestamp}"
        os.makedirs(temp_dir, exist_ok=True)

        for uploaded_file in uploaded_sales_files:
            path = os.path.join(temp_dir, uploaded_file.name)
            with open(path, "wb") as f: f.write(uploaded_file.getbuffer())
            temp_sales_paths.append(path)

        if uploaded_exemption_file:
            temp_exempt_path = os.path.join(temp_dir, uploaded_exemption_file.name)
            with open(temp_exempt_path, "wb") as f: f.write(uploaded_exemption_file.getbuffer())
            logs.append(f"Using supplemental exemption file: {uploaded_exemption_file.name}")

        loader = DataLoader(temp_sales_paths, error_collector)
        raw_df = loader.load_data()
        error_collector.update_summary('total_rows_input', len(raw_df))
        progress_bar.progress(10, text="Step 1/6: Data Loaded.")

        # 2. Standardization
        status_text.text("Step 2/6: Standardizing data...")
        logs.append("Standardizing data...")
        log_box.info("\n".join(logs))
        standardizer = DataStandardizer(raw_df, error_collector)
        std_df = standardizer.standardize()
        progress_bar.progress(30, text="Step 2/6: Standardization Complete.")

        # 3. Nexus Analysis
        status_text.text("Step 3/6: Analyzing nexus thresholds...")
        logs.append("Analyzing nexus thresholds...")
        log_box.info("\n".join(logs))
        nexus_analyzer = NexusAnalyzer(std_df, state_config, error_collector)
        nexus_summary_df = nexus_analyzer.analyze_nexus()
        progress_bar.progress(50, text="Step 3/6: Nexus Analysis Complete.")

        # 4. Exemptions
        status_text.text("Step 4/6: Applying exemptions...")
        logs.append("Applying exemptions...")
        log_box.info("\n".join(logs))
        exemption_manager = ExemptionManager(
            std_df, error_collector,
            exempt_customer_csv=temp_exempt_path if temp_exempt_path and 'customer' in temp_exempt_path.lower() else None,
            exempt_invoice_csv=temp_exempt_path if temp_exempt_path and 'invoice' in temp_exempt_path.lower() else None
            # TODO: Make CSV type explicit or smarter detection
        )
        sales_with_exemptions_df = exemption_manager.apply_exemptions()
        progress_bar.progress(65, text="Step 4/6: Exemptions Applied.")

        # 5. Exposure Calculation
        status_text.text("Step 5/6: Calculating potential exposure...")
        logs.append("Calculating potential exposure...")
        log_box.info("\n".join(logs))
        exposure_calculator = ExposureCalculator(
            nexus_summary_df, sales_with_exemptions_df,
            state_config, vda_option, error_collector
        )
        exposure_df = exposure_calculator.calculate_exposure()
        progress_bar.progress(80, text="Step 5/6: Exposure Calculation Complete.")

        # 6. Reporting
        status_text.text("Step 6/6: Generating reports...")
        logs.append("Generating reports...")
        log_box.info("\n".join(logs))
        reporter = ReportGenerator(
            client_name, error_collector,
            output_dir=run_output_dir, logo_path=logo_path
        )
        report_data_dict = {
            'Nexus_Summary': nexus_summary_df,
            'Exposure_Detail': exposure_df,
            # 'Standardized_Data': sales_with_exemptions_df # Optional: Include cleaned data
        }
        reporter.generate_all_reports(report_data_dict, pdf_summary=f"Nexus analysis report for {client_name}.")
        run_duration = time.time() - run_start_time
        logs.append(f"Report generation complete. Total runtime: {run_duration:.2f} seconds.")
        log_box.info("\n".join(logs))
        progress_bar.progress(100, text="Analysis Complete!")
        status_text.success(f"Analysis Complete! (Duration: {run_duration:.2f}s)")

        # --- Display Results ---
        st.subheader("📈 Run Summary")
        summary_path = os.path.join(run_output_dir, f"run_summary_{reporter.timestamp}.json")
        reject_path = os.path.join(run_output_dir, f"rejected_rows_{reporter.timestamp}.csv")

        summary_data = {}
        if os.path.exists(summary_path):
             with open(summary_path, 'r') as f: summary_data = json.load(f)
             col1, col2, col3, col4 = st.columns(4)
             col1.metric("Rows Input", summary_data.get('total_rows_input', 'N/A'))
             col2.metric("Rows Processed", summary_data.get('rows_processed', 'N/A'))
             col3.metric("Rows Rejected", summary_data.get('rows_rejected', 'N/A'))
             col4.metric("Warnings", len(summary_data.get('warnings', [])))
             st.metric("States Triggering Nexus", summary_data.get('nexus_triggers', 'N/A'))
             st.metric("States with Potential Exposure", summary_data.get('states_with_exposure', 'N/A'))

             if summary_data.get('warnings'):
                 with st.expander("Show Warnings"):
                     st.warning("\n".join(summary_data['warnings']))
        else:
             st.warning("Run summary file not found.")

        # Display Nexus Trigger Summary (unchanged)
        st.subheader("🔔 Nexus Trigger Summary")
        if not nexus_summary_df.empty and 'nexus_triggered' in nexus_summary_df.columns:
             triggered_states = nexus_summary_df[nexus_summary_df['nexus_triggered']].copy()
             if not triggered_states.empty:
                 triggered_states['first_trigger_month_str'] = triggered_states['first_trigger_month'].astype(str)
                 first_triggers = triggered_states.loc[triggered_states.groupby('state')['month_year'].idxmin()]
                 st.dataframe(first_triggers[['state', 'first_trigger_month_str']].rename(columns={'first_trigger_month_str': 'First Trigger Month'}), use_container_width=True)
                 with st.expander("Show Full Nexus Analysis Detail"):
                     st.dataframe(nexus_summary_df, use_container_width=True)
             else:
                 st.info("No states triggered economic nexus based on the data and configuration.")
        else:
             st.info("Nexus analysis did not produce results or 'nexus_triggered' column is missing.")


        # Display Exposure Summary (unchanged formatting)
        st.subheader("💰 Potential Tax Exposure Summary")
        if not exposure_df.empty:
            display_cols = ['state', 'taxable_sales', 'estimated_tax']
            summary_agg = {'taxable_sales': 'sum', 'estimated_tax': 'sum'}

            if vda_option == "Estimate":
                display_cols.extend(['vda_tax', 'vda_interest', 'vda_penalty', 'vda_liability', 'full_liability', 'estimated_vda_savings'])
                summary_agg.update({
                    'vda_tax': 'sum', 'vda_interest': 'sum', 'vda_penalty': 'sum',
                    'vda_liability': 'sum', 'full_liability': 'sum', 'estimated_vda_savings': 'sum'
                })

            state_exposure_summary = exposure_df.groupby('state').agg(summary_agg).reset_index()

            # Formatting dictionary
            format_dict = {col: '${:,.2f}' for col in summary_agg.keys()}
            st.dataframe(state_exposure_summary.style.format(format_dict), use_container_width=True)

            with st.expander("Show Monthly Exposure Detail"):
                st.dataframe(exposure_df, use_container_width=True)
        else:
            st.info("No potential tax exposure calculated.")

        # --- Download Buttons ---
        st.subheader("📥 Download Reports")
        col1, col2 = st.columns(2)

        excel_filename = f"{reporter.base_filename}.xlsx"
        excel_filepath = os.path.join(run_output_dir, excel_filename)
        if os.path.exists(excel_filepath):
            with open(excel_filepath, "rb") as fp:
                col1.download_button(
                    label="Download Excel Report (.xlsx)",
                    data=fp,
                    file_name=excel_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            col1.warning("Excel report not found.")

        if os.path.exists(reject_path) and summary_data.get('rows_rejected', 0) > 0:
             with open(reject_path, "rb") as fp:
                 col2.download_button(
                     label=f"Download Rejected Rows ({summary_data['rows_rejected']}) (.csv)",
                     data=fp,
                     file_name=f"rejected_rows_{reporter.timestamp}.csv",
                     mime="text/csv"
                 )
        else:
             col2.info("No rejected rows generated.")

        # PDF Download (optional)
        # pdf_filename = f"{reporter.base_filename}.pdf"
        # pdf_filepath = os.path.join(run_output_dir, pdf_filename)
        # ... add download button for PDF if generated ...

    except Exception as e:
        status_text.error(f"An error occurred during analysis: {e}")
        st.exception(e) # Show detailed traceback in Streamlit UI
        logs.append(f"ERROR: {e}")
        log_box.error("\n".join(logs))
        progress_bar.progress(100) # Ensure bar completes

    finally:
        # --- Clean up temporary files ---
        try:
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logs.append("Cleaned up temporary files.")
                # log_box.info("\n".join(logs)) # Optional: update log box after cleanup
        except Exception as cleanup_e:
            logs.append(f"Warning: Could not cleanup temporary files in {temp_dir}: {cleanup_e}")
            # log_box.warning("\n".join(logs))

elif uploaded_sales_files and not run_button:
     st.info("Sales files uploaded. Configure settings and click 'Run Nexus Analysis' in the sidebar.")
else:
     st.info("Upload sales data files using the sidebar to begin.")