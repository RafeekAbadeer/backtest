import os
import subprocess
import yaml
import json
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.regime import create_synthetic_suite
from src.validation import run_audit
from src.visualizer import generate_audit_visuals

def main():
    # 1. Setup Stage 5 Output Directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_output = Path(f"results/stage5_{timestamp}")
    stress_results_dir = base_output / "stress_results"
    stress_results_dir.mkdir(parents=True, exist_ok=True)

    # 2. Generate Synthetic Scenarios (Starts Oct 20, 2017)
    print("Generating synthetic data suite...")
    create_synthetic_suite(".") 
    
    with open("data/synthetic/synthetic_manifest.json", "r") as f:
        manifest = json.load(f)

    for entry in manifest:
        scenario_name = entry['filename'].replace(".csv", "")
        run_dir = stress_results_dir / scenario_name
        run_dir.mkdir(parents=True, exist_ok=True)

        # --- HIJACK STAGE 3 SEARCH LOGIC ---
        # Create a fake run folder that run_stage4.py will find as the "latest"
        fake_run_dir = Path("results/run_999999_STAGE5_HIJACK")
        fake_run_dir.mkdir(parents=True, exist_ok=True)
        
        # Build signals that satisfy EntryEngine and Execution Engine
        date_range = pd.date_range(start="2017-10-15", end="2018-01-01", freq='D')
        perfect_signals = pd.DataFrame({
            'date': date_range.strftime('%Y-%m-%d'),
            'daily_signal': True,
            'signal_bool': True,
            'signal': True,
            'Close': 10000.0,
            'ma_value': 9000.0,
            'reason_flags': 'STRESS_TEST'
        })
        perfect_signals.to_csv(fake_run_dir / "daily_signals.csv", index=False)

        # --- DATA PREP ---
        synth_path = f"data/synthetic/{entry['filename']}"
        df_synth = pd.read_csv(synth_path)
        df_synth['Datetime_Obj'] = pd.to_datetime(df_synth['Datetime_Obj']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_synth['Open_Time'] = df_synth['Datetime_Obj']
        df_synth.to_csv(synth_path, index=False)

        # --- CONFIG (Aggressive Entry/Exit for Audit) ---
        dynamic_config = {
            "data": {
                "hourly_input_path": synth_path
            },
            "execution": {"verbose": True},
            "stage4": {
                "initial_capital": 50000.0,
                "dip_pct": 0.0,             # Force entry to ensure trades exist
                "reference_type": "close",
                "tsl_trigger": 0.01, 
                "tsl_distance": 0.01,
                "stop_loss": -0.05,
                "max_hold_days": 1,          # Force exit in 24h to populate trades.csv
                "monthly_dca_amount": 1000.0,
                "dca_conversion_enabled": True
            }
        }

        temp_yaml_path = run_dir / "dynamic_run.yaml"
        with open(temp_yaml_path, "w") as f:
            yaml.dump(dynamic_config, f)

        # --- EXECUTE ---
        print(f"\n>>> Running Scenario: {scenario_name}")
        result = subprocess.run(
            ["python3", "run_stage4.py", str(temp_yaml_path)], 
            capture_output=True, 
            text=True
        )
        
        # --- PRECISE ARTIFACT RECOVERY ---
        output_line = [l for l in result.stdout.split('\n') if "Output directory:" in l]
        if output_line:
            # Extract actual folder: results/run_YYYYMMDD_HHMMSS
            path_str = output_line[0].split("Output directory: ")[1].strip()
            actual_out_base = Path(path_str)
            stage4_out = actual_out_base / "stage4"
            
            for file_name in ["trades.csv", "capital_ledger.csv"]:
                src_file = stage4_out / file_name
                if src_file.exists():
                    shutil.copy(str(src_file), str(run_dir / file_name))
                else:
                    # Fallback recursive search
                    found = list(actual_out_base.glob(f"**/{file_name}"))
                    if found: shutil.copy(str(found[0]), str(run_dir / file_name))
        
        # Cleanup fake signals for next iteration
        if fake_run_dir.exists():
            shutil.rmtree(fake_run_dir)

        # --- AUDIT ---
        trades_path = run_dir / "trades.csv"
        ledger_path = run_dir / "capital_ledger.csv"

        if trades_path.exists() and ledger_path.exists():
            print(f"Audit Initiated for {scenario_name}...")
            # Use run_audit from src.validation
            audit_results = run_audit(str(trades_path), str(ledger_path), dynamic_config)
            
            # Save strictly verdict-only JSON
            with open(run_dir / "audit_report.json", "w") as f:
                json.dump(audit_results, f, indent=4)
            
            # Use visualizer from src.visualizer
            generate_audit_visuals(str(ledger_path), audit_results, str(run_dir))
            print(f"DONE: Results in {run_dir}")
        else:
            print(f"FAILED: No trades. Check logs.")
            # print(result.stdout) # Uncomment for deep debug

if __name__ == "__main__":
    main()