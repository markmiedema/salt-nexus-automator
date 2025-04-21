# SALT Economic Nexus Analysis Automation Tool

This tool automates the process of analyzing state and local tax (SALT) economic nexus obligations based on sales transaction data. It ingests raw sales files, standardizes the data, applies state-specific economic nexus thresholds and rules driven by a central configuration file, calculates potential tax exposure (including optional VDA estimates), and generates client-ready reports and error logs.

## Features

* **Config-Driven Logic:** Nearly all state-specific rules (thresholds, lookback periods, tax rates, marketplace treatment, VDA parameters) are managed in `config/state_config.yaml` for easier updates.
* **Data Ingestion:** Reads CSV and Excel sales files (requires specific columns including `sales_channel`).
* **Data Standardization:**
    * Cleans dates, enforces numeric types, standardizes state/ZIP codes.
    * Includes basic address parsing integration using `usaddress-scourgify`.
    * Correctly handles negative `total_amount` for returns/credits in threshold calculations.
* **Nexus Analysis:**
    * Applies various lookback rules (`rolling_12m`, `calendar_prev_curr`, `rolling_4q`, etc.) as defined in the config.
    * Considers `marketplace_threshold_inclusion` rules from config when evaluating thresholds.
* **Exemption Handling:** Applies exemptions based on source data columns (`is_exempt`, `taxability_code`) and/or supplemental CSV files listing exempt customers or invoices.
* **Exposure Calculation:** Estimates potential tax liability from the point nexus is triggered using configured state tax rates.
* **VDA Estimation (Optional):** Provides a preliminary estimate of Voluntary Disclosure Agreement savings based on configured lookback caps, interest rates, and penalty waiver status.
* **Reporting:** Generates:
    * Multi-sheet Excel reports with nexus summaries and exposure details.
    * Basic PDF cover pages (optional).
    * `rejected_rows.csv`: Detailed log of rows failing validation.
    * `run_summary.json`: Key statistics about the analysis run.
* **Performance:** Targets processing ~1 million transaction rows in under 20 seconds on standard hardware (dependent on data complexity).
* **Streamlit UI (Optional):** Interactive web interface for file uploads (including exemption lists), configuration, running analysis, viewing results, and downloading reports.

## Setup

1.  **Clone the Repository:**
    ```bash
    git clone <repository_url>
    cd salt-nexus-automator
    ```

2.  **Create a Virtual Environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install Dependencies:**
    Ensure you have Python 3.8+ installed.
    ```bash
    pip install -r requirements.txt
    ```
    Key dependencies include: `pandas`, `numpy`, `openpyxl`, `reportlab`, `PyYAML`, `usaddress-scourgify`, `streamlit` (optional for UI).

4.  **Configure Environment Variables:**
    * Copy `.env-example` to `.env`:
        ```bash
        cp .env-example .env
        ```
    * Edit the `.env` file to set your `REPORT_LOGO_PATH` if you want a logo in reports. **Do not commit the `.env` file.**

5.  **Configure State Rules (`config/state_config.yaml`):**
    * **CRITICAL:** This file drives the analysis logic. Review and **update all state entries** with the most current, verified, official state laws, regulations, tax rates, and VDA program details relevant to the analysis period and client situation.
    * The provided `state_config.yaml` structure is an example; **populate it accurately**. Refer to state Department of Revenue websites and reliable SALT resources.
    * Ensure tax rates, lookback rules, thresholds, marketplace inclusion rules, and VDA parameters are correct for each relevant state.

6.  **(Optional) Prepare Exemption Files:**
    * If using supplemental exemption files, format them as CSVs with appropriate headers (`customer_id` or `invoice_number`). See `config/exempt_customers_example.csv`.

## Usage

### Command Line Interface (CLI)

*(Requires `main.py` implementation to parse args and orchestrate the workflow)*

A potential CLI command structure:

```bash
python main.py \
    --input data/raw_sales_data.xlsx \
    --input data/more_sales.csv \
    --config config/state_config.yaml \
    --output output/client_xyz_analysis \
    --client "Client XYZ Corp" \
    --vda-option Estimate \
    # --exempt-customer-list config/client_xyz_exempt_cust.csv