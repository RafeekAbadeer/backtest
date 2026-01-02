#!/usr/bin/env python3
"""
Stage 2 Entry Point - Pure Orchestration Only
Usage: python run_stage2.py config.yaml <run_id>

Responsibilities:
- Load config
- Load data
- Call EntryEngine (black box)
- Save outputs
"""

import sys
import yaml
import pandas as pd
from pathlib import Path
from src.engine import EntryEngine
from src.utils import setup_logging

def main():
    # Parse arguments
    if len(sys.argv) != 3:
        print("Usage: python run_stage2.py <config.yaml> <run_id>")
        print("Example: python run_stage2.py configs/default.yaml run_20240103_120000")
        sys.exit(1)
    
    config_path = Path(sys.argv[1])
    run_id = sys.argv[2]
    
    # Verify inputs exist
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    run_dir = Path("results") / run_id
    if not run_dir.exists():
        print(f"Error: Run directory not found: {run_dir}")
        sys.exit(1)
    
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Setup Stage 2 directory
    stage2_dir = run_dir / "stage2"
    stage2_dir.mkdir(exist_ok=True)
    
    # Setup logging
    logger = setup_logging(run_dir, config['execution']['verbose'], stage="stage2")
    logger.info("Starting Stage 2: Hourly Entry Execution")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Config: {config_path}")
    
    # Define paths
    hourly_data_path = Path(config['data']['hourly_input_path'])
    daily_signals_path = run_dir / "stage1" / "daily_signals.csv"
    entries_path = stage2_dir / "entries.csv"
    summary_path = stage2_dir / "stage2_summary.txt"
    
    # Verify Stage 1 output exists
    if not daily_signals_path.exists():
        logger.error(f"Stage 1 output missing: {daily_signals_path}")
        print(f"Error: {daily_signals_path} not found. Run Stage 1 first.")
        sys.exit(1)
    
    try:
        # Load hourly data
        logger.info(f"Loading hourly data from: {hourly_data_path}")
        df_hourly = pd.read_csv(hourly_data_path)
        
        # Ensure Datetime_Obj is datetime type
        if 'Datetime_Obj' not in df_hourly.columns:
            df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Open_Time'], unit='ms')
        else:
            df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Datetime_Obj'], format='mixed')
        
        logger.info(f"Loaded {len(df_hourly)} hourly candles")
        
        # Load daily signals
        logger.info(f"Loading daily signals from: {daily_signals_path}")
        df_daily = pd.read_csv(daily_signals_path)
        
        # Ensure date column is date type
        if 'date' in df_daily.columns:
            df_daily['date'] = pd.to_datetime(df_daily['date']).dt.date
        
        signal_count = df_daily['daily_signal'].sum() if 'daily_signal' in df_daily.columns else 0
        logger.info(f"Loaded {signal_count} daily signals")
        
        # Initialize EntryEngine
        logger.info("Initializing EntryEngine...")
        entry_engine = EntryEngine(config.get('stage2', {}))
        
        # Call entry logic (black box)
        logger.info("Finding entries...")
        df_entries = entry_engine.find_entries(df_hourly, df_daily)
        entries_found = len(df_entries)
        logger.info(f"Found {entries_found} entries")
        
        # Save entries
        logger.info(f"Saving entries to: {entries_path}")
        df_entries.to_csv(entries_path, index=False)
        
        # Calculate statistics
        missed_signals = signal_count - entries_found
        success_rate = (entries_found / signal_count * 100) if signal_count > 0 else 0
        
        # Save summary
        with open(summary_path, 'w') as f:
            f.write("Stage 2: Hourly Entry Execution Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Total Eligible Signals: {signal_count}\n")
            f.write(f"Entries Found: {entries_found}\n")
            f.write(f"Missed Signals (No Dip): {missed_signals}\n")
            f.write(f"Success Rate: {success_rate:.1f}%\n")
        
        logger.info(f"Summary saved to: {summary_path}")
        logger.info(f"Stage 2 completed successfully")
        logger.info(f"Entry conversion rate: {success_rate:.1f}% ({entries_found}/{signal_count})")
        
        print(f"\n✓ Stage 2 complete. Results in: {stage2_dir}")
        
    except Exception as e:
        logger.error(f"Stage 2 failed: {e}", exc_info=True)
        print(f"\n✗ Stage 2 failed. Check logs in: {run_dir}")
        sys.exit(1)

if __name__ == "__main__":
    main()