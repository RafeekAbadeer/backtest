#!/usr/bin/env python3
"""
Stage 3 Entry Point - Daily Signal Generation
Usage: python run_stage3.py config.yaml

Responsibilities:
- Load config
- Load hourly CSV
- Call generate_daily_signals (black box)
- Save outputs
"""

import sys
import json
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime
from src.engine import generate_daily_signals
from src.utils import setup_logging

def main():
    if len(sys.argv) != 2:
        print("Usage: python run_stage3.py <config.yaml>")
        sys.exit(1)
    
    config_path = Path(sys.argv[1])
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    # Load configuration
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Create timestamped run directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path("results") / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Run directory: {run_dir}")
    
    # Setup logging
    logger = setup_logging(run_dir, config['execution']['verbose'], stage="stage3")
    logger.info("Starting Stage 3: Daily Signal Generation")
    logger.info(f"Config: {config_path}")
    
    # Define paths
    hourly_data_path = Path(config['data']['hourly_input_path'])
    daily_signals_path = run_dir / "daily_signals.csv"
    meta_path = run_dir / "meta.json"
    
    try:
        # Load hourly data
        logger.info(f"Loading hourly data from: {hourly_data_path}")
        df_hourly = pd.read_csv(hourly_data_path)
        logger.info(f"Loaded {len(df_hourly)} hourly candles")
        
        # Ensure Datetime_Obj is datetime type
        if 'Datetime_Obj' not in df_hourly.columns:
            df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Open_Time'], unit='ms')
        else:
            df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Datetime_Obj'], format='mixed')
        
        # Determine date range
        min_date = df_hourly['Datetime_Obj'].min()
        max_date = df_hourly['Datetime_Obj'].max()
        logger.info(f"Date range: {min_date.date()} to {max_date.date()}")
        
        # Call signal generation logic (black box)
        logger.info("Calling generate_daily_signals...")
        df_signals = generate_daily_signals(df_hourly, config)
        
        # Log results
        num_days = len(df_signals)
        num_signals = df_signals['signal_bool'].sum() if 'signal_bool' in df_signals.columns else 0
        logger.info(f"Generated signals for {num_days} days")
        logger.info(f"Total signals: {num_signals}")
        
        # Save daily signals
        signals_path = run_dir / "daily_signals.csv"
        logger.info(f"Saving daily signals to: {signals_path}")
        
        # Ensure date is included as a column, not just index
        if df_signals.index.name or isinstance(df_signals.index, pd.DatetimeIndex):
            df_signals = df_signals.reset_index()
            if 'index' in df_signals.columns:
                df_signals = df_signals.rename(columns={'index': 'date'})
        
        df_signals.to_csv(signals_path, index=False)
        
        # Save metadata
        meta = {
            "timestamp": timestamp,
            "config_file": str(config_path),
            "hourly_candles": len(df_hourly),
            "num_days": num_days,
            "num_signals": int(num_signals),
            "date_range": {
                "start": min_date.strftime('%Y-%m-%d'),
                "end": max_date.strftime('%Y-%m-%d')
            }
        }
        
        logger.info(f"Saving metadata to: {meta_path}")
        with open(meta_path, 'w') as f:
            json.dump(meta, f, indent=2)
        
        logger.info("Stage 3 completed successfully")
        print(f"\n✓ Stage 3 complete. Results in: {run_dir}")
        
    except Exception as e:
        # Fail loudly - no recovery logic
        logger.error(f"Stage 3 failed: {e}", exc_info=True)
        print(f"\n✗ Stage 3 failed. Check logs in: {run_dir}")
        sys.exit(1)

if __name__ == "__main__":
    main()