import pandas as pd
import os
from pathlib import Path

def validate_input_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates data and repairs exchange gaps (like the 163 missing hours).
    Fills gaps with the last known price and 0 volume.
    """
    # Standardize time
    df['Datetime_Obj'] = pd.to_datetime(df['Datetime_Obj'], format='ISO8601')
    df = df.sort_values('Datetime_Obj').drop_duplicates('Datetime_Obj')

    # Create the expected hourly timeline
    full_range = pd.date_range(
        start=df['Datetime_Obj'].min(),
        end=df['Datetime_Obj'].max(),
        freq='h'
    )
    
    if len(df) != len(full_range):
        missing_count = len(full_range) - len(df)
        print(f"--- Data Layer: Repairing {missing_count} missing 1H candles ---")
        
        # Reindex to insert missing timestamps
        df = df.set_index('Datetime_Obj').reindex(full_range)
        
        # Forward fill: Close stays the same, Open/High/Low become that Close
        df['Close'] = df['Close'].ffill()
        df[['Open', 'High', 'Low']] = df[['Open', 'High', 'Low']].ffill()
        
        # Volume for missing period is 0
        df['Volume'] = df['Volume'].fillna(0)
        
        df = df.reset_index().rename(columns={'index': 'Datetime_Obj'})
    
    return df

def get_output_path(base_dir: str, filename: str) -> str:
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    return os.path.join(base_dir, filename)