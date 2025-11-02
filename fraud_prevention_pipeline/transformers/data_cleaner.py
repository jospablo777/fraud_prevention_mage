import pandas as pd
from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """
    df = pd.DataFrame(messages)
    df = df.loc[:, ~df.columns.str.startswith('_')] # Remove columns whose name starts with '_'

    return df
