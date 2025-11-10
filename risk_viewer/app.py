import os
from pathlib import Path
import duckdb
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
import time

# Config (env) 
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/var/lib/mage/data"))
GRAIN = os.getenv("GRAIN", "fraud_high_risk")  # folder name created by our sink
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "50"))

st.set_page_config(page_title="Transactions with High-Risk of fraud", layout="wide")
st.title("Transactions with Higher Risk (of Fraud)")

# Refresh controls
REFRESH_DEFAULT = int(os.getenv("REFRESH_SECONDS", "5"))
c1, c2, c3 = st.columns([1.1, 1, 2.2])
with c1:
    auto_refresh = st.toggle("Auto-refresh", value=True, key="auto_refresh")
with c2:
    refresh_secs = st.number_input("Every (s)", min_value=2, max_value=120,
                                   value=REFRESH_DEFAULT, step=1, key="refresh_secs")
with c3:
    if st.button("Refresh now"):
        st.rerun()

# DuckDB + Parquet view 
con = duckdb.connect(database=":memory:")
con.execute("PRAGMA disable_object_cache;")  # ensure new/updated parquet files are not cached

pattern = str(DATA_ROOT / GRAIN / "year=*" / "month=*" / "*.parquet")

# Create a stable view that exposes the 3 columns we care about.
# If event_time is string in files, cast it; if it’s already TIMESTAMP, try_cast is a no-op.
con.execute(f"""
CREATE OR REPLACE VIEW fraud AS
SELECT
  try_cast(event_time AS TIMESTAMP) AS event_time,
  transaction_id,
  fraud_prob
FROM read_parquet('{pattern}', hive_partitioning=true)
""")

# UI: filters 
stats = con.sql("SELECT min(event_time) AS min_ts, max(event_time) AS max_ts, count(*) AS n FROM fraud").df()
if stats["n"].iloc[0] == 0 or pd.isna(stats["min_ts"].iloc[0]):
    st.warning(f"No data found under: {pattern}")
    st.stop()

min_ts = pd.to_datetime(stats["min_ts"].iloc[0])
max_ts = pd.to_datetime(stats["max_ts"].iloc[0])

# KPI tiles (total rows + latest event time) 
def _pretty_delta(ts: pd.Timestamp | None) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    now = pd.Timestamp.utcnow()
    try:
        delta = now - pd.to_datetime(ts)
    except Exception:
        return "—"
    secs = int(delta.total_seconds())
    if secs < 60:
        return f"{secs}s ago"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"

_total_cases = int(stats["n"].iloc[0])
_latest_ts = pd.to_datetime(stats["max_ts"].iloc[0])

kpi1, kpi2 = st.columns(2)
kpi1.metric("Cases in data", f"{_total_cases:,}")
kpi2.metric(
    "Most recent event",
    _latest_ts.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(_latest_ts) else "—",
    delta=_pretty_delta(_latest_ts),
)

col1, col2, col3, col4 = st.columns([1.4, 1.4, 1, 1])
with col1:
    date_from = st.date_input("From date", min_ts.date(), min_value=min_ts.date(), max_value=max_ts.date())
with col2:
    date_to = st.date_input("To date (inclusive)", max_ts.date(), min_value=min_ts.date(), max_value=max_ts.date())
with col3:
    min_prob = st.slider("Min fraud_prob", 0.0, 1.0, 0.30, 0.01)
with col4:
    limit = st.number_input("Rows", min_value=10, max_value=5000, value=DEFAULT_LIMIT, step=10)

search_tid = st.text_input("Search transaction_id (contains)", "")

# Build params
date_to_exclusive = pd.Timestamp(date_to) + pd.Timedelta(days=1)

query = """
SELECT transaction_id, event_time, fraud_prob
FROM fraud
WHERE event_time >= ? AND event_time < ?
  AND fraud_prob >= ?
  AND (? = '' OR transaction_id ILIKE '%' || ? || '%')
ORDER BY fraud_prob DESC, event_time DESC NULLS LAST
LIMIT ?
"""

df = con.execute(
    query,
    [pd.Timestamp(date_from), date_to_exclusive, float(min_prob), search_tid, search_tid, int(limit)],
).df()

st.caption(f"Root: `{DATA_ROOT}` | Grain: `{GRAIN}` | Pattern: `{pattern}`")
st.dataframe(
    df,
    width="stretch",
)

# Download
st.download_button(
    "Download CSV",
    df.to_csv(index=False).encode("utf-8"),
    file_name="fraud_high_risk_top.csv",
    mime="text/csv",
)

# Auto-rerun the whole script to pick up new data
if st.session_state.get("auto_refresh", False):
    time.sleep(int(st.session_state.get("refresh_secs", REFRESH_DEFAULT)))
    st.rerun()