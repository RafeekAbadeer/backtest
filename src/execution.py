import pandas as pd
import numpy as np

def run_execution_engine(
    hourly_df: pd.DataFrame, 
    daily_signals_df: pd.DataFrame, 
    config: dict
) -> dict:
    s4_cfg = config.get('stage4', {})
    dip_pct = s4_cfg.get('dip_pct', 0.02)
    tsl_trigger = s4_cfg.get('tsl_trigger', 0.05)
    sld = s4_cfg.get('tsl_distance', 0.03) 
    max_hold_days = s4_cfg.get('max_hold_days', 90)
    initial_capital_pool = s4_cfg.get('initial_capital', 10000)
    dca_amount = s4_cfg.get('monthly_dca_amount', 1000)

    # Data Alignment
    df_1h = hourly_df.sort_values('Open_Time').copy()
    df_1h['Datetime_Obj'] = pd.to_datetime(df_1h['Datetime_Obj'])
    
    signals = daily_signals_df.copy()
    if 'date' in signals.columns:
        signals = signals.set_index('date')
    signals.index = pd.to_datetime(signals.index).normalize()
    signal_bool_lookup = signals[~signals.index.duplicated(keep='last')]['signal_bool'].to_dict()

    # Inside src/execution.py -> run_execution_engine()
    # ... after signal_bool_lookup is created ...

    print(f"DEBUG ENGINE: First 5 Signal Keys: {list(signal_bool_lookup.keys())[:5]}")
    print(f"DEBUG ENGINE: First 5 Hourly Datetimes: {df_1h['Datetime_Obj'].iloc[:5].tolist()}")
    print(f"DEBUG ENGINE: Signal Check for first hour: {(df_1h['Datetime_Obj'].iloc[0] - pd.Timedelta(days=1)).normalize() in signal_bool_lookup}")

    # State
    free_capital = initial_capital_pool
    active_trades = []
    closed_trades = []
    capital_ledger = []
    consumed_signals = set()
    last_month = None

    for _, candle in df_1h.iterrows():
        current_time = candle['Datetime_Obj']
        current_month = current_time.month
        
        # --- CALENDAR-BASED REPLENISHMENT ---
        if last_month is not None and current_month != last_month:
            # Identify "Stuck" trades with remaining exposure
            stuck_trades = [
                t for t in active_trades 
                if (current_time - t['entry_datetime']).days > max_hold_days 
                and t['remaining_exposed_capital'] > 0
            ]
            
            if stuck_trades:
                total_stuck_val = sum(t['remaining_exposed_capital'] for t in stuck_trades)
                injection = min(total_stuck_val, dca_amount)
                
                # Apply Injection: Reduces exposure per trade, moves capital to free_capital
                remaining_to_inject = injection
                for t in stuck_trades:
                    if remaining_to_inject <= 0: break
                    reduction = min(t['remaining_exposed_capital'], remaining_to_inject)
                    
                    # Update Explicit Invariants
                    t['remaining_exposed_capital'] -= reduction
                    t['replenished_capital_cumulative'] += reduction
                    remaining_to_inject -= reduction
                
                free_capital += injection
        
        last_month = current_month
        signal_date_ts = (current_time - pd.Timedelta(days=1)).normalize()

        # --- A. EXIT LOGIC (TSL) ---
        remaining_trades_list = []
        for trade in active_trades:
            # TSL Peak Tracking
            if not trade['tsl_active']:
                if candle['High'] >= trade['entry_price'] * (1 + tsl_trigger):
                    trade['tsl_active'] = True
                    trade['peak_price'] = candle['High']
            else:
                trade['peak_price'] = max(trade['peak_price'], candle['High'])
            
            # TSL Breach Check
            if trade['tsl_active']:
                tsl_price = trade['peak_price'] * (1 - sld)
                if candle['Low'] <= tsl_price:
                    trade['exit_datetime'] = current_time
                    trade['exit_price'] = min(candle['Open'], tsl_price)
                    
                    # Proportional Return Logic
                    perf_ratio = trade['exit_price'] / trade['entry_price']
                    # Capital returned = current exposure multiplied by performance
                    exit_payout = trade['remaining_exposed_capital'] * perf_ratio
                    free_capital += exit_payout
                    
                    closed_trades.append(trade)
                    continue
            
            remaining_trades_list.append(trade)
        active_trades = remaining_trades_list

        # --- B. ENTRY LOGIC (Dip-Based) ---
        if signal_date_ts in signal_bool_lookup and signal_date_ts not in consumed_signals:
            if signal_bool_lookup[signal_date_ts] == True and free_capital > 0:
                # Reference price derived from the close of the previous day in 1H data
                prev_day_close = df_1h[df_1h['Datetime_Obj'] < current_time.normalize()]['Close'].iloc[-1] if not df_1h[df_1h['Datetime_Obj'] < current_time.normalize()].empty else candle['Open']
                dip_target = prev_day_close * (1 - dip_pct)
                
                entry_price = None
                if candle['Open'] <= dip_target: entry_price = candle['Open']
                elif candle['Low'] <= dip_target: entry_price = dip_target
                
                if entry_price:
                    active_trades.append({
                        'entry_datetime': current_time,
                        'entry_price': entry_price,
                        'initial_capital': free_capital,
                        'replenished_capital_cumulative': 0.0,
                        'remaining_exposed_capital': free_capital,
                        'tsl_active': False,
                        'peak_price': entry_price,
                        'signal_date': signal_date_ts
                    })
                    free_capital = 0
                    consumed_signals.add(signal_date_ts)

        # --- C. LEDGER UPDATE ---
        capital_ledger.append({
            'timestamp': current_time,
            'free_capital': free_capital,
            'active_trades_count': len(active_trades),
            'total_exposure': sum(t['remaining_exposed_capital'] for t in active_trades)
        })

    # Ensure trades is a DataFrame even if closed_trades is empty
    if isinstance(closed_trades, list):
        trades_df = pd.DataFrame(closed_trades)
    else:
        trades_df = closed_trades

    # Explicitly define columns to ensure a 0-row file has a valid header
    required_columns = [
        'entry_datetime', 'exit_datetime', 'entry_price', 'exit_price', 
        'initial_capital', 'remaining_exposed_capital', 'replenished_capital_cumulative',
        'tsl_active', 'signal_date'
    ]

    for col in required_columns:
        if col not in trades_df.columns:
            trades_df[col] = None

    return {
        "trades": trades_df, 
        "capital_ledger": pd.DataFrame(capital_ledger)
    }

if __name__ == "__main__":
    import argparse
    import yaml
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Execution Engine CLI")
    parser.add_argument("--hourly", type=str, required=True)
    parser.add_argument("--signals", type=str, required=True)
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    df_h = pd.read_csv(args.hourly)
    df_s = pd.read_csv(args.signals)
    
    results = run_execution_engine(df_h, df_s, config)
    
    out_path = Path(args.output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    results['trades'].to_csv(out_path / "trades.csv", index=False)
    results['capital_ledger'].to_csv(out_path / "capital_ledger.csv", index=False)
    print(f"Execution artifacts saved to {args.output_dir}")