import uuid
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from sdv.sampling import Condition

def sample_with_base_rate(synthesizer, n_rows: int, fraud_rate: float,
                          rng: np.random.Generator | int | None = None,
                          shuffle: bool = True) -> pd.DataFrame:
    if not (0.0 <= float(fraud_rate) <= 1.0):
        raise ValueError("fraud_rate must be in [0, 1].")
    if isinstance(rng, (int, np.integer)) or rng is None:
        rng = np.random.default_rng(rng)

    n1 = int(rng.binomial(n_rows, float(fraud_rate)))
    n0 = n_rows - n1

    if n_rows == 0:
        return pd.DataFrame([])

    conds = []
    if n0: conds.append(Condition(num_rows=n0, column_values={"Class": 0}))
    if n1: conds.append(Condition(num_rows=n1, column_values={"Class": 1}))

    out = synthesizer.sample_from_conditions(conditions=conds)
    out["Class"] = out["Class"].astype(int)

    # batch timestamp & IDs
    now = datetime.now(timezone.utc)
    iso = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    ms = int(now.timestamp() * 1000)
    out.insert(0, "transaction_id", [uuid.uuid4().hex for _ in range(len(out))])
    out["event_time"] = iso
    out["event_time_ms"] = ms

    if shuffle and len(out) > 1:
        seed = int(rng.integers(0, 2**32 - 1))
        out = out.sample(frac=1, random_state=seed).reset_index(drop=True)
    return out
