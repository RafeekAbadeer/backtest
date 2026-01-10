import sys
import os
from pathlib import Path

# 1. MODIFY PATH FIRST
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

# 2. NOW PERFORM LOCAL IMPORTS
import pandas as pd
import json
from src.execution import run_execution_engine

def run_parameter_sweep(
    hourly_data_path: str,
    daily_signals_path: str,
    base_config: dict,
    param_grid: dict,
    output_dir: str
):
    """
    Mechanically executes Stage 4 across a parameter grid.
    Saves raw results per combination and a summary table.
    """
    df_hourly = pd.read_csv(hourly_data_path)
    df_daily = pd.read_csv(daily_signals_path)
    
    sweep_path = Path(output_dir) / "sweep_results"
    sweep_path.mkdir(parents=True, exist_ok=True)
    
    summary_data = []

    # Cartesian product of parameter grid
    import itertools
    keys = param_grid.keys()
    values = param_grid.values()
    combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]

    for i, combo in enumerate(combinations):
        # Update local config copy
        current_config = base_config.copy()
        for k, v in combo.items():
            current_config['stage4'][k] = v
        
        # Execute Black Box Engine
        result = run_execution_engine(df_hourly, df_daily, current_config)
        
        trades_df = result['trades']
        ledger_df = result['capital_ledger']
        
        # Save raw artifacts for this specific run
        run_id = f"run_{i:03d}"
        run_dir = sweep_path / run_id
        run_dir.mkdir(exist_ok=True)
        trades_df.to_csv(run_dir / "trades.csv", index=False)
        ledger_df.to_csv(run_dir / "ledger.csv", index=False)
        
        # Extract raw end-state accounting metrics
        final_equity = ledger_df['free_capital'].iloc[-1] + ledger_df['total_exposure'].iloc[-1]
        max_exp = ledger_df['total_exposure'].max()
        
        row = {
            "run_id": run_id,
            "final_equity": final_equity,
            "max_exposure": max_exp,
            "trade_count": len(trades_df)
        }
        row.update(combo)
        summary_data.append(row)

    # Save summary table
    pd.DataFrame(summary_data).to_csv(sweep_path / "sweep_summary.csv", index=False)

def execute_synthetic_battery(synthetic_dir: str, base_config: dict, output_dir: str):
    """
    Executes the base config against all generated synthetic scenarios.
    """
    manifest_path = Path(synthetic_dir) / "synthetic_manifest.json"
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
        
    battery_path = Path(output_dir) / "synthetic_battery"
    battery_path.mkdir(parents=True, exist_ok=True)
    
    # In a real stress test, synthetic signals would be needed. 
    # For this runner, we assume signals are derived or provided per scenario.
    # This acts as the structural template for battery testing.

if __name__ == "__main__":
    import argparse
    import yaml
    import json

    parser = argparse.ArgumentParser(description="Parameter Sweeper CLI")
    parser.add_argument("--hourly", type=str, required=True)
    parser.add_argument("--signals", type=str, required=True)
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--grid", type=str, required=True, help="JSON path for param_grid")
    parser.add_argument("--output-dir", type=str, required=True)
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        base_config = yaml.safe_load(f)
    with open(args.grid, 'r') as f:
        param_grid = json.load(f)

    run_parameter_sweep(args.hourly, args.signals, base_config, param_grid, args.output_dir)
    print(f"Parameter sweep complete. Results in {args.output_dir}")