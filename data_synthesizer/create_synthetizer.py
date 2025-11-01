import pandas as pd
import numpy as np
from sdv.metadata import Metadata
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.sampling import Condition
from pathlib import Path
import json
from rich import print


# Read the data to train our synthetizer
fraud_data = pd.read_csv('original_data/creditcard.csv')


# Prep data
df_model = fraud_data.drop(columns=['Time']).copy()
df_model['Class'] = df_model['Class'].astype(int)


# Build metadata
metadata = Metadata.detect_from_dataframe(
    data=df_model,
    table_name='creditcard'
)
# Ensure the target is categorical (so 0/1 isn’t treated as numeric)
metadata.update_column(column_name='Class', sdtype='categorical')
metadata.validate()
# We save metadata for reproducibility
Path("artifacts").mkdir(parents=True, exist_ok=True) # Create artifacts directory
metadata_path = "artifacts/creditcard_fraud_metadata.json"
metadata.save_to_json(metadata_path, mode="overwrite")


# Train synthetizer
# GaussianCopula is fast, stable, and supports efficient conditional sampling for rare classes
synth = GaussianCopulaSynthesizer(
    metadata = Metadata.load_from_json(filepath=metadata_path),
    enforce_min_max_values=True,
    enforce_rounding=True,
)
print('[yellow]Fitting the synthesizer, this might take some minutes 🤖 bip bop...[/yellow] \n')
synth.fit(df_model)


# Save artifacts (generator + metadata + base stats)
# Save synthetizer
synth_path = "artifacts/creditcard_fraud_gc.pkl"
synth.save(synth_path)

# Save stats
base_rate = float(df_model['Class'].mean())
with open("artifacts/creditcard_fraud_stats.json", "w") as f:
    json.dump({"class_positive_rate": base_rate, "n_rows": int(len(df_model))}, f, indent=2)

print(f"[green]🛟  Saved synthesizer:[/green] {synth_path}")
print(f"[green]🛟  Saved metadata:[/green]    {metadata_path}")
print(f"[green]👀 Observed fraud rate in real data:[/green] [teal]{base_rate:.6f}[/teal]")