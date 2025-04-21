# src/utils.py
import json
import yaml # PyYAML needed
import pandas as pd
import logging
import sys
import os

def setup_logging(log_level=logging.INFO, log_to_file=False, log_dir="output"):
    """Sets up basic logging to stdout and optionally to a file."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Stream Handler (stdout)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)

    # File Handler (optional)
    if log_to_file:
        os.makedirs(log_dir, exist_ok=True)
        log_filepath = os.path.join(log_dir, f"run_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_filepath)
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)
        logging.info(f"Logging initialized. Also logging to file: {log_filepath}")
    else:
         logging.info("Logging initialized (stdout only).")


def load_yaml_config(path: str) -> dict:
    """Loads configuration from a YAML file."""
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
            logging.info(f"Successfully loaded configuration from {path}")
            # Basic validation (check if it's a dictionary)
            if not isinstance(config, dict):
                raise ValueError("Configuration file did not load as a dictionary.")
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration file {path}: {e}")
        raise
    except ValueError as e:
         logging.error(f"Configuration file format error: {e}")
         raise

class ErrorCollector:
    """Collects rejected rows and run summary information."""
    def __init__(self):
        self.rejected_rows = []
        self.warnings = []
        self.summary = {
            "start_time": pd.Timestamp.now(),
            "end_time": None,
            "duration_seconds": None,
            "total_rows_input": 0,
            "rows_processed": 0,
            "rows_rejected": 0,
            "nexus_triggers": 0,
            "states_with_exposure": 0,
            "warnings": [],
            "warnings_count": 0
        }
        logging.info("ErrorCollector initialized.")

    def add_rejected_row(self, row_data: dict, reason: str):
        """Adds a row that failed validation."""
        # Ensure row_data is serializable (convert Period/Timestamp)
        entry = {k: str(v) if isinstance(v, (pd.Period, pd.Timestamp)) else v for k, v in row_data.items()}
        entry['rejection_reason'] = reason
        self.rejected_rows.append(entry)
        self.summary['rows_rejected'] += 1

    def add_warning(self, message: str):
        """Adds a general warning."""
        if message not in self.warnings: # Avoid duplicate warnings
            self.warnings.append(message)
            self.summary['warnings'].append(message)
            self.summary['warnings_count'] = len(self.warnings)
            logging.warning(message) # Also log warnings

    def get_rejected_rows_df(self) -> pd.DataFrame:
        """Returns collected rejected rows as a DataFrame."""
        if not self.rejected_rows:
            return pd.DataFrame()
        return pd.DataFrame(self.rejected_rows)

    def update_summary(self, key: str, value: any):
        """Updates a specific key in the summary dictionary."""
        self.summary[key] = value

    def finalize_summary(self):
        """Calculates duration and finalizes summary."""
        self.summary["end_time"] = pd.Timestamp.now()
        self.summary["duration_seconds"] = (self.summary["end_time"] - self.summary["start_time"]).total_seconds()
        logging.info(f"Run summary finalized. Duration: {self.summary['duration_seconds']:.2f}s")


    def get_summary(self) -> dict:
        """Returns the final summary dictionary."""
        # Ensure timestamps are strings for JSON serialization if needed later
        summary_copy = self.summary.copy()
        summary_copy["start_time"] = str(summary_copy["start_time"])
        summary_copy["end_time"] = str(summary_copy["end_time"])
        return summary_copy