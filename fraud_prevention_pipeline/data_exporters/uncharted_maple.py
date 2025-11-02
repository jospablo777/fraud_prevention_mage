from __future__ import annotations
from mage_ai.io.file import FileIO
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
from uuid import uuid4
from mage_ai.streaming.sinks.base_python import BasePythonSink
from typing import Callable, Dict, List, Union
import logging
import os


if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink


# ================================
# Helpers
# ================================

def _to_df(msgs: Union[pd.DataFrame, List[Dict], List]) -> pd.DataFrame:
    # Case 1: already a DataFrame
    if isinstance(msgs, pd.DataFrame):
        return msgs

    # Case 2: [DataFrame]
    if isinstance(msgs, list) and len(msgs) == 1 and isinstance(msgs[0], pd.DataFrame):
        return msgs[0]

    # Case 3: list of dicts (common from source)
    if isinstance(msgs, list) and msgs and isinstance(msgs[0], dict):
        return pd.DataFrame(msgs)

    # Case 4: list of lists of dicts (batched)
    if isinstance(msgs, list):
        flat: List[Dict] = []
        for m in msgs:
            if isinstance(m, list):
                flat.extend(m)
            elif isinstance(m, dict):
                flat.append(m)
            elif isinstance(m, pd.DataFrame):
                flat.extend(m.to_dict(orient='records'))
        return pd.DataFrame(flat)

    # Fallback: single dict-like
    return pd.DataFrame([msgs])

def save_partitioned_by_month(
    df: pd.DataFrame,
    base_dir: str,
    grain: str,
    filename: str = "data.parquet",
    drop_partitions_in_file: bool = True,
    verbose: bool = True,
    datetime_col: str = "event_time",
) -> list[str]:
    """
    Save df into <base_dir>/<grain>/year=YYYY/month=MM/<filename> using Mage's FileIO.

    Parameters
    ----------
    df : DataFrame
        Must contain either columns 'year' and 'month' (ints), or a datetime column
        specified by `datetime_col` (default 'event_time').
    base_dir : str
        Base folder (e.g., '../data/features').
    grain : str
        Subfolder (e.g., 'fraud_high_risk').
    filename : str
        File name inside each partition (default 'data.parquet').
    drop_partitions_in_file : bool
        Drop the 'year' and 'month' columns inside each file (Hive-style).
    verbose : bool
        Pass-through to FileIO(verbose=...).
    datetime_col : str
        Name of a datetime column to derive partitions from if 'year'/'month' are absent.

    Returns
    -------
    list[str] : absolute file paths written
    """
    df = df.copy()

    # Derive year/month if needed
    if not ({"year", "month"} <= set(df.columns)):
        if datetime_col not in df.columns:
            raise ValueError(
                "Provide either 'year' and 'month' columns, "
                f"or a datetime column '{datetime_col}'."
            )
        # Parse and derive
        dt = pd.to_datetime(df[datetime_col], errors="coerce", utc=True)
        df["year"] = dt.dt.year.astype("Int32")
        df["month"] = dt.dt.month.astype("Int32")

    # Ensure integer types (avoid folders like '2018.0')
    df["year"] = df["year"].astype("int32")
    df["month"] = df["month"].astype("int32")

    # Drop rows where partition keys are missing (if any)
    df = df.dropna(subset=["year", "month"])

    root = Path(base_dir).expanduser().resolve() / grain
    root.mkdir(parents=True, exist_ok=True)

    written: list[str] = []

    # Group and write per (year, month)
    for (y, m), part in df.groupby(["year", "month"], sort=True):
        dest_dir = root / f"year={int(y)}" / f"month={int(m):02d}"
        dest_dir.mkdir(parents=True, exist_ok=True)

        out_df = part.drop(columns=["year", "month"]) if drop_partitions_in_file else part
        out_df = out_df.reset_index(drop=True)  # avoid storing index

        dest_path = dest_dir / filename

        # Prefer writing WITHOUT index
        try:
            FileIO(verbose=verbose).export(
                out_df, str(dest_path), format="parquet", index=False
            )
        except TypeError:
            # Fallback: pandas direct
            out_df.to_parquet(dest_path, engine="pyarrow", index=False)

        written.append(str(dest_path))

    return written

@streaming_sink
class CustomSink(BasePythonSink):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        cfg = getattr(self, "config", {}) or {}

        # Order of precedence:
        # 1) block config: config.base_dir
        # 2) env var: MAGE_EXPORT_BASE_DIR
        # 3) hard default (inside container): /var/lib/mage/data
        self.base_dir = (
            cfg.get("base_dir")
            or os.getenv("MAGE_EXPORT_BASE_DIR")
            or "/var/lib/mage/data"
        )
        self.grain = cfg.get("grain", "fraud_high_risk")
        self.filename_prefix = cfg.get("filename_prefix", "part")

        self.logger = logging.getLogger("CustomSink")
        if not self.logger.handlers:
            h = logging.StreamHandler()
            from logging import Formatter
            h.setFormatter(Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
            self.logger.addHandler(h)
        self.logger.setLevel(logging.INFO)
        self.logger.info(f"CustomSink base_dir resolved to: {self.base_dir}")

    def batch_write(self, messages: List[Dict]):
        df = _to_df(messages)
        if df.empty:
            self.logger.info("CustomSink: no rows to write; skipping.")
            return

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        fname = f"{self.filename_prefix}-{ts}-{uuid4().hex[:8]}.parquet"

        paths = save_partitioned_by_month(
            df=df,
            base_dir=self.base_dir,
            grain=self.grain,
            filename=fname,
            drop_partitions_in_file=True,
            verbose=True,
            datetime_col="event_time",
        )
        self.logger.info(f"CustomSink: wrote {len(df)} rows to {len(paths)} file(s): {paths}")

    def write(self, data: Dict, **kwargs):
        self.batch_write([data])

