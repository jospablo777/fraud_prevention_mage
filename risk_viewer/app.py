import os
from pathlib import Path
import duckdb
import pandas as pd
import streamlit as st

# Config (env) 
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/var/lib/mage/data"))
GRAIN = os.getenv("GRAIN", "fraud_high_risk")  # folder name created by our sink
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "50"))

st.set_page_config(page_title="Transactions with High-Risk of fraud", layout="wide")
st.title("Transactions with Higher Risk (of Fraud)")

# DuckDB + Parquet view 
con = duckdb.connect(database=":memory:")
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
    use_container_width=True,
)

# Download
st.download_button(
    "Download CSV",
    df.to_csv(index=False).encode("utf-8"),
    file_name="fraud_high_risk_top.csv",
    mime="text/csv",
)
