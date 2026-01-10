import pandas as pd
import numpy as np

def aggregate_hourly_to_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates 1H OHLCV data into 1D UTC candles.
    Ensures only complete 24-hour periods are used.
    """
    df = hourly_df.copy()
    
    # Ensure Datetime_Obj is recognized
    df['Datetime_Obj'] = pd.to_datetime(df['Datetime_Obj'])
    df['date_key'] = df['Datetime_Obj'].dt.date
    
    # Standard Aggregation
    daily = df.groupby('date_key').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'Datetime_Obj': 'count'  # Counter for completeness
    })
    
    # Strict Constraint: Drop incomplete days (requires exactly 24 hours)
    daily = daily[daily['Datetime_Obj'] == 24].copy()
    daily.drop(columns=['Datetime_Obj'], inplace=True)
    daily.index.name = 'date'
    
    return daily

def _calculate_rsi(series: pd.Series, period: int) -> pd.Series:
    """Helper for RSI calculation to ensure deterministic causal results."""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    # Avoid division by zero
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def generate_daily_signals(hourly_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Main Logic: Aggregation -> Indicators -> Signal Evaluation.
    Uses read-only namespaced config['signals'].
    
    Note: Drawdown is computed for context but does NOT filter signals.
    """
    # Extract namespaced config (read-only)
    sig_cfg = config.get('signals', {})
    
    # 1. Aggregation
    df = aggregate_hourly_to_daily(hourly_df)
    
    # 2. Indicators (Causal)
    ma_period = sig_cfg.get('ma_period', 50)
    rsi_period = sig_cfg.get('rsi_period', 14)
    
    # Moving Average
    if sig_cfg.get('ma_type', 'SMA').upper() == 'EMA':
        df['ma_value'] = df['Close'].ewm(span=ma_period, adjust=False).mean()
    else:
        df['ma_value'] = df['Close'].rolling(window=ma_period).mean()
        
    # RSI
    df['rsi'] = _calculate_rsi(df['Close'], rsi_period)
    
    # Peak/Drawdown (Causal - Context Only)
    df['peak'] = df['Close'].cummax()
    df['drawdown_pct'] = (df['Close'] - df['peak']) / df['peak']
    
    # 3. Signal Evaluation
    rsi_threshold = sig_cfg.get('rsi_threshold', 70)
    
    # Evaluation Logic (Drawdown removed from boolean gating)
    condition_ma = df['Close'] > df['ma_value']
    condition_rsi = df['rsi'] < rsi_threshold
    
    df['signal_bool'] = (condition_ma & condition_rsi).fillna(False)
    
    # 4. Reason Flags (Audit Trail)
    df['reason_flags'] = ""
    # We use .fillna(True) for the bitwise check to avoid NaN issues in flag reporting
    df.loc[~(condition_ma.fillna(False)), 'reason_flags'] += "Below_MA;"
    df.loc[~(condition_rsi.fillna(False)), 'reason_flags'] += "Overbought_RSI;"
    
    # Final cleanup to return only required columns
    cols = [
        'Open', 'High', 'Low', 'Close', 'Volume', 
        'ma_value', 'rsi', 'peak', 'drawdown_pct', 'signal_bool', 'reason_flags'
    ]
    return df[cols]

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser(description="Daily Signal Generation CLI")
    parser.add_argument("--input", type=str, required=True, help="Path to hourly OHLCV CSV")
    parser.add_argument("--config", type=str, required=True, help="Path to YAML config")
    parser.add_argument("--output", type=str, required=True, help="Path to save daily signals CSV")
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    df_hourly = pd.read_csv(args.input)
    # Ensure Datetime_Obj is converted before passing to logic
    df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Datetime_Obj'])
    
    df_signals = generate_daily_signals(df_hourly, config)
    df_signals.to_csv(args.output, index=True) # Index contains 'date'
    print(f"Signals generated and saved to {args.output}")