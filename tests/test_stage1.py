#!/usr/bin/env python3
"""
Stage 1 Entry Point - Execution Skeleton
Usage: python run_stage1.py configs/default.yaml
"""

import sys
import yaml
import shutil
from pathlib import Path
from src.utils import setup_logging, create_run_directory, save_config_snapshot
from src.engine import run_backtest

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
    
    # Setup logging
    logger = setup_logging(run_dir, config['execution']['verbose'])
    logger.info(f"Starting Stage 1 execution")
    logger.info(f"Config: {config_path}")
    
    # Save configuration snapshot
    if config['output']['save_config_snapshot']:
        save_config_snapshot(config, config_path, run_dir)
        logger.info("Configuration snapshot saved")
    
    # Run the backtest engine
    try:
        results = run_backtest(config, logger)
        logger.info("Execution completed successfully")
        
        # Save summary
        if config['output']['create_summary']:
            summary_path = run_dir / "summary.txt"
            with open(summary_path, 'w') as f:
                f.write(f"Stage 1 Execution Summary\n")
                f.write(f"========================\n")
                f.write(f"Status: SUCCESS\n")
                f.write(f"Results: {results}\n")
            logger.info(f"Summary saved to {summary_path}")
        
        print(f"\n✓ Execution complete. Results in: {run_dir}")
        
    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
        print(f"\n✗ Execution failed. Check logs in: {run_dir}")
        sys.exit(1)

if __name__ == "__main__":
    main()