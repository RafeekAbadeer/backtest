#!/usr/bin/env python3
"""
Stage 4: Execution Engine Runner
Orchestrates the backtest execution simulation.
"""

import sys
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

from src.engine import run_execution_engine
from src.utils import setup_logging, create_run_directory, save_config_snapshot

def find_latest_stage3_run():
    """Find the most recent Stage 3 run directory."""
    results_dir = Path("results")
    if not results_dir.exists():
        return None
    
    # Get all run directories
    run_dirs = [d for d in results_dir.iterdir() if d.is_dir() and d.name.startswith("run_")]
    
    # Filter to those that have daily_signals.csv (Stage 3 output)
    stage3_runs = [d for d in run_dirs if (d / "daily_signals.csv").exists()]
    
    if not stage3_runs:
        return None
    
    # Return most recent
    return sorted(stage3_runs, key=lambda x: x.name)[-1]

def main():
    if len(sys.argv) < 2:
        print("Usage: python run_stage4.py <config_path>")
        sys.exit(1)

    config_path = Path(sys.argv[1])
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create run directory
    run_dir = create_run_directory()
    stage_dir = run_dir / "stage4"
    stage_dir.mkdir(exist_ok=True)
    
    # Setup logging
    logger = setup_logging(run_dir, verbose=config['execution'].get('verbose', True), stage="stage4")
    logger.info("=" * 60)
    logger.info("Stage 4: Execution Engine")
    logger.info("=" * 60)
    
    # Load hourly data
    hourly_path = Path(config['data']['hourly_input_path'])
    logger.info(f"Loading hourly data from: {hourly_path}")
    df_hourly = pd.read_csv(hourly_path)
    df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Datetime_Obj'], format='mixed')
    logger.info(f"Loaded {len(df_hourly):,} hourly candles")
    
    # Find and load Stage 3 daily signals
    stage3_run_dir = find_latest_stage3_run()
    
    if stage3_run_dir is None:
        logger.error("No Stage 3 output found in results/ directory")
        logger.error("Please run Stage 3 first: python run_stage3.py configs/default.yaml")
        sys.exit(1)
    
    daily_signals_path = stage3_run_dir / "daily_signals.csv"
    logger.info(f"Using Stage 3 output from: {stage3_run_dir.name}")
    logger.info(f"Loading daily signals from: {daily_signals_path}")
    
    # Stage 3 stores date as index - read it properly
    df_daily_signals = pd.read_csv(daily_signals_path)
    
    # The first column is the date (unnamed index from CSV)
    if 'Unnamed: 0' in df_daily_signals.columns:
        df_daily_signals = df_daily_signals.rename(columns={'Unnamed: 0': 'date'})
        df_daily_signals['date'] = pd.to_datetime(df_daily_signals['date'])
    elif df_daily_signals.columns[0] not in ['Open', 'High', 'Low', 'Close']:
        # First column is the date with some other name
        df_daily_signals = df_daily_signals.rename(columns={df_daily_signals.columns[0]: 'date'})
        df_daily_signals['date'] = pd.to_datetime(df_daily_signals['date'])
    else:
        # No date column found - this shouldn't happen
        logger.error("Cannot find date column in Stage 3 output!")
        logger.error(f"Columns found: {df_daily_signals.columns.tolist()}")
        sys.exit(1)
    
    # Count signals
    signal_col = 'signal_bool' if 'signal_bool' in df_daily_signals.columns else 'signal'
    signal_count = df_daily_signals[signal_col].sum() if signal_col in df_daily_signals.columns else len(df_daily_signals)
    
    logger.info(f"Loaded {len(df_daily_signals):,} daily rows ({signal_count} signals)")
    
    # === DIAGNOSTIC OUTPUT (REMOVE AFTER DEBUGGING) ===
    logger.info("=== DIAGNOSTIC CHECK ===")
    logger.info(f"Hourly columns: {df_hourly.columns.tolist()}")
    logger.info(f"Hourly date range: {df_hourly['Datetime_Obj'].min()} to {df_hourly['Datetime_Obj'].max()}")
    logger.info(f"Hourly shape: {df_hourly.shape}")
    logger.info(f"\nDaily signals columns: {df_daily_signals.columns.tolist()}")
    
    if 'date' in df_daily_signals.columns:
        logger.info(f"Daily signals date range: {df_daily_signals['date'].min()} to {df_daily_signals['date'].max()}")
    else:
        logger.info(f"WARNING: 'date' column not found in daily signals!")
    
    logger.info(f"Daily signals shape: {df_daily_signals.shape}")
    logger.info(f"\nFirst 3 signal rows where signal=True:")
    signal_col = 'signal_bool' if 'signal_bool' in df_daily_signals.columns else 'signal'
    if signal_col in df_daily_signals.columns:
        logger.info(f"\n{df_daily_signals[df_daily_signals[signal_col] == True].head(3).to_string()}")
    logger.info(f"\nConfig execution keys: {list(config.get('execution', {}).keys())}")
    logger.info(f"Config stage4 keys: {list(config.get('stage4', {}).keys())}")
    logger.info("=== END DIAGNOSTIC ===\n")
    # === END DIAGNOSTIC ===
    # === DETAILED DATA INSPECTION ===
    logger.info("=== PRE-ENGINE DATA CHECK ===")
    logger.info(f"Passing to engine:")
    logger.info(f"  - Hourly rows: {len(df_hourly)}")
    logger.info(f"  - Daily rows: {len(df_daily_signals)}")
    logger.info(f"  - Signals with True: {df_daily_signals['signal_bool'].sum()}")
    
    logger.info(f"\nSample daily signal row (first True signal):")
    first_signal = df_daily_signals[df_daily_signals['signal_bool'] == True].iloc[0]
    for key, val in first_signal.to_dict().items():
        logger.info(f"  {key}: {val} (type: {type(val).__name__})")
    
    logger.info(f"\nSample hourly rows for signal date {first_signal['date'].date()}:")
    signal_date = first_signal['date']
    matching_hourly = df_hourly[df_hourly['Datetime_Obj'].dt.date == signal_date.date()]
    logger.info(f"  Found {len(matching_hourly)} hourly candles for this day")
    if len(matching_hourly) > 0:
        logger.info(f"  First hourly candle: {matching_hourly.iloc[0][['Datetime_Obj', 'Open', 'High', 'Low', 'Close']].to_dict()}")
    
    logger.info(f"\nConfig being passed to engine:")
    logger.info(f"  config['execution']: {config.get('execution', {})}")
    logger.info(f"  config['stage4']: {config.get('stage4', {})}")
    logger.info("=== END PRE-ENGINE CHECK ===\n")

    # Run execution engine
    logger.info("Starting execution engine...")
    try:
        # Try with positional arguments (most compatible)
        result = run_execution_engine(df_hourly, df_daily_signals, config)
    except TypeError as e:
        logger.error(f"Function signature mismatch: {e}")
        logger.error("Please verify run_execution_engine() parameter names in src/engine.py")
        logger.error("Expected: run_execution_engine(df_hourly, df_daily_signals, config)")
        sys.exit(1)
    
    df_trades = result['trades']
    df_capital_ledger = result['capital_ledger']
    metadata = result.get('metadata', {})
    
    logger.info(f"Execution complete: {len(df_trades)} trades processed")
    
    # Save outputs
    trades_path = stage_dir / "trades.csv"
    capital_path = stage_dir / "capital_ledger.csv"
    
    df_trades.to_csv(trades_path, index=False)
    logger.info(f"Saved trades to: {trades_path}")
    
    df_capital_ledger.to_csv(capital_path, index=False)
    logger.info(f"Saved capital ledger to: {capital_path}")
    
    # Create summary
    summary_path = stage_dir / "stage4_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("Stage 4 Execution Summary\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Total Trades: {len(df_trades)}\n")
        
        if 'status' in df_trades.columns:
            status_counts = df_trades['status'].value_counts()
            for status, count in status_counts.items():
                f.write(f"  {status}: {count}\n")
        
        if 'exit_reason' in df_trades.columns:
            exit_counts = df_trades['exit_reason'].value_counts()
            f.write(f"\nExit Reasons:\n")
            for reason, count in exit_counts.items():
                f.write(f"  {reason}: {count}\n")
        
        if metadata:
            f.write(f"\nMetadata:\n")
            for key, value in metadata.items():
                f.write(f"  {key}: {value}\n")
    
    logger.info(f"Saved summary to: {summary_path}")
    
    # Save config snapshot
    if config.get('output', {}).get('save_config_snapshot', True):
        save_config_snapshot(config, config_path, run_dir)
    
    logger.info("=" * 60)
    logger.info(f"Stage 4 complete. Output directory: {run_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()