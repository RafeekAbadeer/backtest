#!/usr/bin/env python3
"""
Stage 1 Entry Point - Execution Skeleton
Usage: python run_stage1.py configs/default.yaml
"""

import sys
import yaml
import shutil
import pandas as pd
from pathlib import Path
from src.utils import setup_logging, create_run_directory, save_config_snapshot
from src.engine import Stage1Engine

def main():
    if len(sys.argv) != 2:
        print("Usage: python run_stage1.py <config.yaml>")
        sys.exit(1)
    
    config_path = Path(sys.argv[1])
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    # Load configuration
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Create timestamped run directory
    run_dir = create_run_directory()
    print(f"Run directory: {run_dir}")
    
    # Create stage1 subdirectory
    stage1_dir = run_dir / "stage1"
    stage1_dir.mkdir(exist_ok=True)
    
    # Setup logging
    logger = setup_logging(run_dir, config['execution']['verbose'])
    logger.info(f"Starting Stage 1 execution")
    logger.info(f"Config: {config_path}")
    
    # Save configuration snapshot
    if config['output']['save_config_snapshot']:
        save_config_snapshot(config, config_path, run_dir)
        logger.info("Configuration snapshot saved")
    
    # Define paths
    hourly_data_path = Path(config['data']['hourly_input_path'])
    daily_candles_path = stage1_dir / "daily_candles.csv"
    daily_signals_path = stage1_dir / "daily_signals.csv"
    
    try:
        # Load hourly data
        logger.info(f"Loading hourly data from: {hourly_data_path}")
        df_hourly = pd.read_csv(hourly_data_path)
        logger.info(f"Loaded {len(df_hourly)} hourly candles")
        
        # Ensure Datetime_Obj is datetime type
        if 'Datetime_Obj' not in df_hourly.columns:
            # Create from Open_Time if missing
            df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Open_Time'], unit='ms')
        else:
            # Convert to datetime if it exists but wrong type
            df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Datetime_Obj'], format='mixed')
        
        # Initialize Stage1Engine with config
        engine = Stage1Engine(config.get('stage1', {}))
        logger.info("Stage1Engine initialized")
        
        # Aggregate to daily
        logger.info("Aggregating to daily candles...")
        df_daily = engine.aggregate_daily(df_hourly)
        logger.info(f"Created {len(df_daily)} daily candles")
        
        # Compute indicators
        logger.info("Computing indicators...")
        df_daily = engine.compute_indicators(df_daily)
        logger.info("Indicators computed")
        
        # Apply signal logic
        logger.info("Applying signal logic...")
        df_daily = engine.apply_signal_logic(df_daily)
        signal_count = df_daily['daily_signal'].sum()
        logger.info(f"Generated {signal_count} daily signals")
        
        # Save outputs
        logger.info(f"Saving daily candles to: {daily_candles_path}")
        df_daily.to_csv(daily_candles_path, index=False)
        
        logger.info(f"Saving daily signals to: {daily_signals_path}")
        df_daily.to_csv(daily_signals_path, index=False)
        
        logger.info("Stage 1 execution completed successfully")
        
        # Save summary
        if config['output']['create_summary']:
            summary_path = run_dir / "summary.txt"
            with open(summary_path, 'w') as f:
                f.write(f"Stage 1 Execution Summary\n")
                f.write(f"========================\n")
                f.write(f"Status: SUCCESS\n")
                f.write(f"Hourly candles processed: {len(df_hourly)}\n")
                f.write(f"Daily candles created: {len(df_daily)}\n")
                f.write(f"Daily signals generated: {signal_count}\n")
            logger.info(f"Summary saved to {summary_path}")
        
        print(f"\n✓ Stage 1 complete. Results in: {run_dir}")
        
    except Exception as e:
        logger.error(f"Stage 1 execution failed: {e}", exc_info=True)
        print(f"\n✗ Stage 1 failed. Check logs in: {run_dir}")
        sys.exit(1)

if __name__ == "__main__":
    main()