import pandas as pd 
from catboost import CatBoostClassifier
from catboost import Pool
from typing import Dict, List, Union

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


# ================================
# Helpers
# ================================

FEATURES = [
    'V1','V2','V3','V4','V5','V6','V7','V8','V9','V10',
    'V11','V12','V13','V14','V15','V16','V17','V18','V19','V20',
    'V21','V22','V23','V24','V25','V26','V27','V28','Amount'
]

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

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """
    df = _to_df(messages).copy()
    # Load ML (CatBoost) model
    predictor = CatBoostClassifier()
    predictor.load_model("ml_artifacts/catboost_fraud.cbm")

    # Ensure predictors only has the model features (same order as FEATURES)
    predictors = df[FEATURES]

    # Build a Pool without labels and predict probabilities
    pred_pool = Pool(predictors)
    fraud_prob = predictor.predict_proba(pred_pool)[:, 1]

    # Append probabilities 
    prediction_data = df[['transaction_id', 'event_time']].copy()
    prediction_data['fraud_prob'] = fraud_prob


    # Ensure event_time is datetime and sort by it
    prediction_data['event_time'] = pd.to_datetime(prediction_data['event_time'], errors='coerce')
    prediction_data = prediction_data.sort_values('event_time')

    # Transactions with probabilities over 40% (this assuming it is a well calibrated probability)
    high_risk_transactions = prediction_data[prediction_data.fraud_prob > 0.2].copy()

    return high_risk_transactions
