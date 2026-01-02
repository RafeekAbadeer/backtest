"""
Utility functions for logging, directory management, and configuration
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path

def create_run_directory():
    """Create timestamped directory for this run"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path("results") / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir

def setup_logging(run_dir, verbose=True, stage="stage1"):
    """Setup dual logging to file and console"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{stage}_{timestamp}.log"
    
    # Create logger
    logger_name = f"backtest_engine_{stage}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # File handler (detailed)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Console handler (concise)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    # Also save log in run directory
    run_log = run_dir / f"{stage}_execution.log"
    shutil.copy2(log_file, run_log)
    
    return logger

def save_config_snapshot(config, config_path, run_dir):
    """Save copy of config file used for this run"""
    snapshot_path = run_dir / "config_snapshot.yaml"
    shutil.copy2(config_path, snapshot_path)
    
    # Also save metadata
    metadata_path = run_dir / "metadata.txt"
    with open(metadata_path, 'w') as f:
        f.write(f"Execution timestamp: {datetime.now().isoformat()}\n")
        f.write(f"Config file: {config_path}\n")
        f.write(f"Random seed: {config.get('execution', {}).get('random_seed', 'N/A')}\n")