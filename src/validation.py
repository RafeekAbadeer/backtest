import pandas as pd
import numpy as np
import json
from pathlib import Path

def run_audit(trades_path, ledger_path, config):
    # Load raw artifacts
    df_l = pd.read_csv(ledger_path)
    
    # Read trades; if header-only, it returns a 0-row DataFrame
    df_t = pd.read_csv(trades_path)

    # GUARD: Handle zero-trade scenario
    if df_t.empty:
        return {
            "counters": {
                "negative_balance_observed": int((df_l['free_capital'] < -1e-6).sum()),
                "broken_invariants_observed": 0,
                "off_schedule_injections_observed": 0
            },
            "flags": {
                "accounting_breach": int((df_l['free_capital'] < -1e-6).sum()) > 0
            },
            "note": "Zero trades executed; invariant audit skipped."
        }
    
    # Raw Counter Logic
    # 1. Negative Balance Detection
    neg_balance_count = int((df_l['free_capital'] < -1e-6).sum())
    
    # 2. Conservation Invariant Check
    # (initial == remaining + replenished)
    broken_invariants = int((np.abs(df_t['initial_capital'] - 
                          (df_t['remaining_exposed_capital'] + 
                           df_t['replenished_capital_cumulative'])) > 1e-6).sum())
    
    # 3. Timing Check (Off-schedule injections)
    df_l['timestamp'] = pd.to_datetime(df_l['timestamp'])
    # Only check hours where free_capital increased without trade count decreasing
    injections = df_l[(df_l['free_capital'].diff() > 1e-6) & (df_l['active_trades_count'].diff() >= 0)]
    off_schedule = int((injections['timestamp'].dt.hour != 0).sum())

    return {
        "counters": {
            "negative_balance_observed": neg_balance_count,
            "broken_invariants_observed": broken_invariants,
            "off_schedule_injections_observed": off_schedule
        },
        "flags": {
            "accounting_breach": neg_balance_count > 0 or broken_invariants > 0 or off_schedule > 0
        }
    }

def save_audit_report(results: dict, output_dir: str):
    output_path = Path(output_dir) / "audit_report.json"
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=4)

if __name__ == "__main__":
    import argparse
    import yaml
    import json

    parser = argparse.ArgumentParser(description="Validation Audit CLI")
    parser.add_argument("--trades", type=str, required=True)
    parser.add_argument("--ledger", type=str, required=True)
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--output", type=str, required=True, help="Path to save JSON report")
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    audit_results = run_audit(args.trades, args.ledger, config)
    
    with open(args.output, 'w') as f:
        json.dump(audit_results, f, indent=4)
    print(f"Audit report saved to {args.output}")