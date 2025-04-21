# src/ingestion.py
"""Data ingestion module for Economic‑Nexus project.

Reads one or many CSV / Excel files, applies basic schema validation,
and returns a concatenated ``pandas.DataFrame`` ready for
standardisation / rule‑engine processing.

It adapts its loading strategy on‑the‑fly based on file size:
•  Files under ``ONE_SHOT_LIMIT`` are read in one pass with
   ``low_memory=False`` for stable dtype inference.
•  Larger CSVs stream in ``chunksize=CHUNK_SIZE`` increments to
   keep memory usage predictable.  Excel files are always loaded
   in one shot (xlsx rarely exceed 100 MB and ``read_excel`` lacks
   streaming support).

Environment overrides
---------------------
MAX_ONESHOT_BYTES   Maximum file size (bytes) to read in one go.
CSV_CHUNKSIZE       Rows per chunk when streaming large CSVs.

If the variables are missing the defaults below apply.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Sequence, Union

import pandas as pd

# Project‑level helper class for surfacing issues to the caller
from src.utils import ErrorCollector

# ──────────────────────────────────────────────────────────────
# Tuning knobs
# ──────────────────────────────────────────────────────────────

ONE_SHOT_LIMIT: int = int(os.getenv("MAX_ONESHOT_BYTES", 100 * 1024 * 1024))  # 100 MB default
CHUNK_SIZE: int = int(os.getenv("CSV_CHUNKSIZE", 100_000))                    # 100 k rows default

# Optional dtype map – declare columns you know for sure. This can
# be extended or pulled in from a YAML schema later.
DTYPE_MAP: Dict[str, str] = {
    "invoice_number": "string",
    "total_amount": "float64",
    "zip_code": "string",
}


class DataLoader:
    """Handles loading sales/transaction extracts from disk."""

    def __init__(
        self,
        file_paths: Union[Path, str, Sequence[Union[Path, str]]],
        error_collector: ErrorCollector,
    ) -> None:
        self.file_paths: List[Path] = (
            [Path(p) for p in file_paths]
            if isinstance(file_paths, (list, tuple))
            else [Path(file_paths)]
        )
        self.error_collector = error_collector

        # Required/optional columns will eventually reside in a single
        # schema module but are in‑lined here for brevity.
        self.required_columns: List[str] = [
            "date",
            "invoice_number",
            "invoice_date",
            "total_amount",
            "customer_name",
            "street_address",
            "city",
            "state",
            "zip_code",
            "sales_channel",
        ]
        self.optional_columns: List[str] = [
            "is_exempt",
            "taxability_code",
            "customer_id",
        ]

        logging.info("DataLoader initialised for %s file(s)", len(self.file_paths))

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def load_data(self) -> pd.DataFrame:
        """Orchestrates reading & basic validation for every configured path."""
        all_frames: List[pd.DataFrame] = []
        total_rows_input = 0

        for path in self.file_paths:
            logging.info("→ Loading %s", path)
            if not path.exists():
                self._warn(f"Input file not found: {path}. Skipping.")
                continue

            try:
                df = self._read_file(path)
            except Exception as exc:  # noqa: BLE001 – surface & continue
                self._warn(
                    f"Error reading {path.name}: {exc}. Skipping file.",
                    exc_info=True,
                )
                continue

            if df.empty:
                self._warn(f"File {path.name} produced 0 rows after validation. Skipping.")
                continue

            total_rows_input += len(df)
            all_frames.append(df)
            logging.info("✓ %s rows accepted from %s", len(df), path.name)

        self.error_collector.update_summary("total_rows_input", total_rows_input)

        if not all_frames:
            self._warn("No valid data loaded. Returning empty DataFrame.")
            return pd.DataFrame(columns=self.required_columns + self.optional_columns)

        combined = pd.concat(all_frames, ignore_index=True)
        logging.info("✅ Concatenated %s rows from %s file(s)", len(combined), len(all_frames))
        return combined

    # ──────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────

    def _read_file(self, path: Path) -> pd.DataFrame:
        """Choose stream vs one‑shot strategy based on suffix & size."""
        suffix = path.suffix.lower()

        if suffix == ".csv":
            return self._read_csv(path)
        if suffix in {".xlsx", ".xls"}:
            return self._read_excel(path)

        raise ValueError(f"Unsupported file extension: {path.suffix}")

    # CSV ─────────────────────────────────────────────────────
    def _read_csv(self, path: Path) -> pd.DataFrame:
        file_size = path.stat().st_size
        streaming = file_size > ONE_SHOT_LIMIT

        if not streaming:
            logging.debug("One‑shot CSV read (%s bytes)", file_size)
            df = pd.read_csv(path, low_memory=False, dtype=DTYPE_MAP, engine="pyarrow")
            return self._postprocess(df, path.name)

        logging.debug(
            "Streaming CSV read (%s bytes) in %s‑row chunks", file_size, CHUNK_SIZE
        )
        chunks = pd.read_csv(
            path,
            chunksize=CHUNK_SIZE,
            low_memory=True,
            dtype=DTYPE_MAP,
            engine="pyarrow",
        )

        processed_chunks: List[pd.DataFrame] = [self._postprocess(chunk, path.name) for chunk in chunks]
        return pd.concat(processed_chunks, ignore_index=True)

    # Excel ───────────────────────────────────────────────────
    @staticmethod
    def _read_excel(path: Path) -> pd.DataFrame:
        # Excel reading lacks chunking, but files are usually smaller.
        return pd.read_excel(path, engine="openpyxl", dtype=DTYPE_MAP or None)

    # Post‑read validation / cleaning ─────────────────────────
    def _postprocess(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Enforce lowercase cols, trim spaces, validate required columns."""
        df.columns = df.columns.str.lower().str.strip()

        missing = [col for col in self.required_columns if col not in df.columns]
        if missing:
            self._warn(
                f"{source_name} missing required columns: {missing}. Rejecting file."
            )
            return pd.DataFrame()

        # Type coercion guard – ignore errors to leave bad cells as NaN/None.
        if DTYPE_MAP:
            df = df.astype({k: v for k, v in DTYPE_MAP.items() if k in df.columns}, errors="ignore")

        return df

    # Logging / error‑collector sugar ─────────────────────────
    def _warn(self, message: str, *, exc_info: bool = False) -> None:  # noqa: D401
        """Add warning via ErrorCollector + python‑logging."""
        self.error_collector.add_warning(message)
        logging.warning(message, exc_info=exc_info)
