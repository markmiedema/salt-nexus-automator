# src/salt_nexus_automator/ingestion.py
"""Data ingestion module for Economic-Nexus project."""

from __future__ import annotations
import logging
import os
from pathlib import Path
from typing import List, Dict, Union, Sequence

import pandas as pd
from .utils import ErrorCollector

# -- knobs --
ONE_SHOT_LIMIT = int(os.getenv("MAX_ONESHOT_BYTES", 100 * 1024 * 1024))
CHUNK_SIZE     = int(os.getenv("CSV_CHUNKSIZE",      100_000))

DTYPE_MAP: Dict[str, str] = {
    "invoice_number": "string",
    "total_amount":   "float64",
    "zip_code":       "string",
}

class DataLoader:
    """Load one or many CSV / Excel extracts and return a validated DataFrame."""

    def __init__(
        self,
        file_paths: Union[str, Path, Sequence[Union[str, Path]]] | None = None,
        error_collector: ErrorCollector | None = None,
    ) -> None:
        if file_paths is None:
            self.file_paths: List[Path] = []
        elif isinstance(file_paths, (str, Path)):
            self.file_paths = [Path(file_paths)]
        else:
            self.file_paths = [Path(p) for p in file_paths]

        self.error_collector = error_collector
        # Define required columns
        self.required_columns = [
            "date","invoice_number","invoice_date","total_amount","customer_name",
            "street_address","city","state","zip_code","sales_channel",
        ]
        self.optional_columns = ["is_exempt","taxability_code","customer_id"]
        logging.info("DataLoader initialised for %s file(s)", len(self.file_paths))

    # -- public --
    def load_data(self) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        total_rows = 0

        for path in self.file_paths:
            if not path.exists():
                self._warn(f"Input file not found: {path}. Skipping."); continue
            try:
                df = self._read_file(path)
            except Exception as exc:
                self._warn(f"Error reading {path.name}: {exc}. Skipping.", exc_info=True); continue
            if df.empty:
                self._warn(f"{path.name} produced 0 rows after validation. Skipping."); continue
            total_rows += len(df); frames.append(df)
            logging.info("✓ %s rows accepted from %s", len(df), path.name)

        if self.error_collector:
            self.error_collector.update_summary("total_rows_input", total_rows)

        if not frames:
            self._warn("No valid data loaded. Returning empty DataFrame.")
            # Return an empty DataFrame with all potential columns for consistency
            return pd.DataFrame(columns=self.required_columns + self.optional_columns)

        return pd.concat(frames, ignore_index=True)

    # -- internals --
    def _read_file(self, path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".csv":    return self._read_csv(path)
        if path.suffix.lower() in {".xlsx",".xls"}: return self._read_excel(path)
        raise ValueError(f"Unsupported file extension: {path.suffix}")

    def _read_csv(self, path: Path) -> pd.DataFrame:
        if path.stat().st_size <= ONE_SHOT_LIMIT:
            df = pd.read_csv(path, low_memory=False, dtype=DTYPE_MAP, engine="pyarrow")
            return self._postprocess(df, path.name)
        chunks = pd.read_csv(path, chunksize=CHUNK_SIZE, dtype=DTYPE_MAP, engine="pyarrow")
        return pd.concat((self._postprocess(c, path.name) for c in chunks), ignore_index=True)

    @staticmethod
    def _read_excel(path: Path) -> pd.DataFrame:
        # Note: dtype is applied here, but might need adjustment for specific Excel nuances
        return pd.read_excel(path, engine="openpyxl", dtype=DTYPE_MAP or None)

    def _postprocess(self, df: pd.DataFrame, source_name: str = "<inline>") -> pd.DataFrame:
        """
        Applies basic cleaning (lowercase/strip columns) and validates required columns.
        Renames 'sales_channel' to 'channel' if present.
        """
        original_columns = df.columns.tolist() # Keep original column names for the check

        # Lower-case and strip column names
        df.columns = df.columns.str.lower().str.strip()

        # Check for missing required columns *before* renaming 'sales_channel'
        # We check against the lower/stripped column names
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            self._warn(f"{source_name} missing required columns: {missing}. Rejecting."); return pd.DataFrame()

        # Rename legacy column -> canonical 'channel' *after* the missing check
        if "sales_channel" in df.columns and "channel" not in df.columns:
            df.rename(columns={"sales_channel":"channel"}, inplace=True)

        # dtype coercion - apply to columns that exist in the DataFrame
        if DTYPE_MAP:
            # Filter DTYPE_MAP to only include columns present in the DataFrame
            dtype_map_present = {k: v for k, v in DTYPE_MAP.items() if k in df.columns}
            if dtype_map_present:
                 df = df.astype(dtype_map_present, errors="ignore")

        return df

    def _warn(self, msg: str, *, exc_info: bool=False) -> None:
        if self.error_collector: self.error_collector.add_warning(msg)
        logging.warning(msg, exc_info=exc_info)
