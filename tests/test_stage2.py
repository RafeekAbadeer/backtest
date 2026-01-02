import pytest
import pandas as pd
from src.engine import EntryEngine
from typing import Dict, Any, List

def test_entry_trigger_logic():
    # 1. Setup Mock Daily Signal (Permission for '2023-01-01')
    daily_signals = pd.DataFrame([{
        'date': pd.to_datetime('2023-01-01').date(),
        'daily_signal': True,
        'Close': 100.0,
        'ma_value': 95.0
    }])

    # 2. Setup Mock Hourly Data for '2023-01-02' (The day after signal)
    # Candle 0-3: No dip
    # Candle 4: Dips to 97 (Target is 98 for a 2% dip)
    hourly_data = []
    for i in range(10):
        low_val = 100.0 if i != 4 else 96.0 
        hourly_data.append({
            'Open_Time': 1672617600000 + (i * 3600000),
            'Datetime_Obj': pd.to_datetime('2023-01-02') + pd.Timedelta(hours=i),
            'Open': 100.0,
            'High': 101.0,
            'Low': low_val,
            'Close': 99.0
        })
    df_1h = pd.DataFrame(hourly_data)

    # 3. Initialize Engine with 2% dip from 'close'
    config = {"entry_dip_pct": 0.02, "entry_ref_type": "close"}
    engine = EntryEngine(config)
    
    # 4. Run Logic
    entries = engine.find_entries(df_1h, daily_signals)

    # 5. Assertions
    assert len(entries) == 1, "Should have triggered exactly one entry"
    assert entries.iloc[0]['entry_price'] == 98.0, "Entry price should be the limit price (98.0)"
    assert entries.iloc[0]['entry_datetime'] == "2023-01-02 04:00:00", "Should trigger at the 4th hour"

def test_no_signal_no_entry():
    # If daily_signal is False, no entries should be generated
    daily_signals = pd.DataFrame([{'date': pd.to_datetime('2023-01-01').date(), 'daily_signal': False, 'Close': 100.0}])
    df_1h = pd.DataFrame([{'Datetime_Obj': pd.to_datetime('2023-01-02 05:00:00'), 'Low': 50.0, 'Open_Time': 12345, 'Open': 100.0}])
    
    engine = EntryEngine({"entry_dip_pct": 0.02, "entry_ref_type": "close"})
    entries = engine.find_entries(df_1h, daily_signals)
    
    assert len(entries) == 0, "Should not enter if daily signal is False"