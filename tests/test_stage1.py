import pytest
import pandas as pd
import numpy as np
from src.engine import Stage1Engine
from src.utils import validate_input_data

def create_mock_1h_data(rows=48):
    """Creates valid 1H data for 2 full days."""
    times = pd.date_range("2023-01-01", periods=rows, freq="h")
    df = pd.DataFrame({
        'Datetime_Obj': times,
        'Open': 100.0, 'High': 110.0, 'Low': 90.0, 'Close': 105.0, 'Volume': 1000
    })
    return df

def test_daily_aggregation():
    df_1h = create_mock_1h_data(rows=48)
    engine = Stage1Engine({"ma_period": 2})
    daily = engine.aggregate_daily(df_1h)
    
    # Assert 48 hours becomes 2 days
    assert len(daily) == 2
    # Verify Volume sum (24 * 1000)
    assert daily.loc[0, 'Volume'] == 24000

def test_gap_detection():
    df_1h = create_mock_1h_data(rows=48)
    df_broken = df_1h.drop(index=5) # Create a gap
    with pytest.raises(ValueError, match="missing 1H candles"):
        validate_input_data(df_broken)

def test_indicator_lookback():
    df_1h = create_mock_1h_data(rows=120) # 5 days
    engine = Stage1Engine({"ma_period": 3})
    daily = engine.aggregate_daily(df_1h)
    daily = engine.compute_indicators(daily)
    
    # First 2 days should be NaN for a 3-period MA
    assert np.isnan(daily.loc[0, 'ma_value'])
    assert np.isnan(daily.loc[1, 'ma_value'])
    assert not np.isnan(daily.loc[2, 'ma_value'])