import pandas as pd
import datetime
from src.engine import Stage1Engine
from src.utils import validate_input_data, get_output_path

# Example Config Structure
CONFIG = {
    "ma_period": 50,
    "ma_type": "SMA",
    "rsi_period": 14,
    "peak_window": 252,
    "rsi_threshold": 30,
    "dd_threshold": -0.15
}

def run():
    # Setup paths
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"results/{timestamp}/stage1/"
    input_file = "data/source_1h.csv" # Standard location

    # 1. Load and Validate
    df_raw = pd.read_csv(input_file)
    validate_input_data(df_raw)
    
    # 2. Run Engine
    engine = Stage1Engine(CONFIG)
    daily_df = engine.aggregate_daily(df_raw)
    daily_df = engine.compute_indicators(daily_df)
    daily_df = engine.apply_signal_logic(daily_df)
    
    # 3. Emit Outputs
    daily_candles_path = get_output_path(output_dir, "daily_candles.csv")
    daily_signals_path = get_output_path(output_dir, "daily_signals.csv")
    
    # daily_candles.csv: Basic aggregation
    daily_df[['date', 'Open', 'High', 'Low', 'Close', 'Volume']].to_csv(
        daily_candles_path, index=False
    )
    
    # daily_signals.csv: The Permission Table
    signal_cols = ['date', 'daily_signal', 'Close', 'ma_value', 'rsi', 'peak', 'drawdown']
    daily_df[signal_cols].to_csv(daily_signals_path, index=False)

if __name__ == "__main__":
    run()