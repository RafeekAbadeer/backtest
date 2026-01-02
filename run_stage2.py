import pandas as pd
import os
import glob
from typing import Dict, Any
from src.engine import EntryEngine
from src.utils import get_output_path

# Assuming CONFIG is consistent with your project needs
CONFIG = {
    "entry_dip_pct": 0.02,
    "entry_ref_type": "close"
}

def get_latest_run_dir(base_results_dir: str = "results") -> str:
    """Finds the most recently created run directory in the results folder."""
    all_runs = [os.path.join(base_results_dir, d) for d in os.listdir(base_results_dir) 
                if os.path.isdir(os.path.join(base_results_dir, d))]
    
    if not all_runs:
        raise FileNotFoundError(f"No run directories found in {base_results_dir}")
    
    # Sort by modification time to find the newest run folder
    latest_run = max(all_runs, key=os.path.getmtime)
    return latest_run

def run_stage2(run_dir: str, config: Dict[str, Any]):
    """Execution logic for Stage 2."""
    # 1. Path Construction (pointing to stage1 subfolder)
    daily_path = os.path.join(run_dir, "stage1", "daily_signals.csv")
    hourly_path = "data/clean_combined_crypto_data.csv" 
    
    if not os.path.exists(daily_path):
        raise FileNotFoundError(f"Could not find Stage 1 results at {daily_path}")

    # 2. Load Data
    df_daily = pd.read_csv(daily_path)
    df_daily['date'] = pd.to_datetime(df_daily['date']).dt.date
    
    df_1h = pd.read_csv(hourly_path)
    # Ensure Datetime_Obj is parsed correctly for comparison
    df_1h['Datetime_Obj'] = pd.to_datetime(df_1h['Datetime_Obj'], format='ISO8601')
    
    # 3. Process Entries
    engine = EntryEngine(config)
    df_entries = engine.find_entries(df_1h, df_daily)
    
    # 4. Save to stage2 subfolder
    output_path = get_output_path(os.path.join(run_dir, "stage2"), "entries.csv")
    df_entries.to_csv(output_path, index=False)
    
    print(f"--- Stage 2 Complete ---")
    print(f"Input: {daily_path}")
    print(f"Output: {output_path}")
    print(f"Entries Found: {len(df_entries)}")

if __name__ == "__main__":
    try:
        latest_run = get_latest_run_dir()
        run_stage2(latest_run, CONFIG)
    except Exception as e:
        print(f"Error executing Stage 2: {e}")