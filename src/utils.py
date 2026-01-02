import pandas as pd
import os
from pathlib import Path

def validate_input_data(df: pd.DataFrame):
    """
    Validates the 1H source data for schema integrity and continuity.
    Fails loudly if 1H candles are missing or timestamps are duplicated.
    """
    required_cols = ['Datetime_Obj', 'Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in df.columns for col in required_cols):
        missing = set(required_cols) - set(df.columns)
        raise ValueError(f"Schema Mismatch. Missing columns: {missing}")

    # Ensure datetime conversion
    df['Datetime_Obj'] = pd.to_datetime(df['Datetime_Obj'])
    
    # Check for duplicates
    if df['Datetime_Obj'].duplicated().any():
        raise ValueError("Data Integrity Error: Duplicate 1H timestamps detected.")

    # Check for gaps (hourly resolution)
    df = df.sort_values('Datetime_Obj')
    expected_range = pd.date_range(
        start=df['Datetime_Obj'].min(),
        end=df['Datetime_Obj'].max(),
        freq='h'
    )
    if len(df) != len(expected_range):
        actual_set = set(df['Datetime_Obj'])
        expected_set = set(expected_range)
        missing = sorted(list(expected_set - actual_set))
        raise ValueError(f"Data Integrity Error: {len(missing)} missing 1H candles. First missing: {missing[0]}")

def get_output_path(base_dir: str, filename: str) -> str:
    """Ensures the results/timestamp/stage1/ directory exists and returns full path."""
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    return os.path.join(base_dir, filename)