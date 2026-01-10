#!/usr/bin/env python3
"""
Stage 5: Validation, Stress Testing, and Robustness Analysis
Mechanical orchestration only - no analytics or interpretation.
"""

import sys
import yaml
import subprocess
import logging
from pathlib import Path
from datetime import datetime


def create_stage5_directory():
    """Create Stage 5 run directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path("results") / f"stage5_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def setup_stage5_logging(run_dir):
    """Setup logging for Stage 5."""
    log_file = run_dir / "stage5_execution.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)


def run_command(cmd, logger, description):
    """Execute subprocess command and handle errors."""
    logger.info(f"Running: {description}")
    logger.info(f"Command: {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"Command failed with return code {result.returncode}")
        logger.error(f"stderr: {result.stderr}")
        return False
    
    if result.stdout:
        logger.info(f"stdout: {result.stdout}")
    
    return True


def check_trades_generated(trades_csv_path, logger):
    """Check if trades.csv has data rows (not just header)."""
    if not trades_csv_path.exists():
        logger.error(f"Trades file not found: {trades_csv_path}")
        return False
    
    with open(trades_csv_path, 'r') as f:
        lines = f.readlines()
    
    # Header + at least one data row
    if len(lines) < 2:
        logger.error(f"Integration failure: {trades_csv_path} has no data rows (only header)")
        return False
    
    logger.info(f"Trades file valid: {len(lines) - 1} trades generated")
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_stage5.py <config_path>")
        sys.exit(1)
    
    config_path = Path(sys.argv[1])
    
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create Stage 5 directory
    run_dir = create_stage5_directory()
    logger = setup_stage5_logging(run_dir)
    
    logger.info("=" * 70)
    logger.info("Stage 5: Validation, Stress Testing, and Robustness Analysis")
    logger.info("=" * 70)
    logger.info(f"Run directory: {run_dir}")
    logger.info(f"Config: {config_path}")
    
    # Save config snapshot
    config_snapshot_path = run_dir / "config_snapshot.yaml"
    with open(config_snapshot_path, 'w') as f:
        yaml.dump(config, f)
    logger.info(f"Config snapshot saved: {config_snapshot_path}")
    
    # Get Stage 5 configuration
    stage5_config = config.get('stage5', {})
    stress_scenarios = stage5_config.get('stress_scenarios', ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8'])
    sweep_grid = stage5_config.get('sweep_parameters', {})
    
    # =====================================================================
    # STRESS TESTS
    # =====================================================================
    
    logger.info("\n" + "=" * 70)
    logger.info("STRESS TESTS")
    logger.info("=" * 70)
    
    stress_dir = run_dir / "stress_results"
    stress_dir.mkdir(exist_ok=True)
    
    for scenario in stress_scenarios:
        logger.info(f"\n--- Stress Scenario: {scenario} ---")
        
        # Step 1: Generate synthetic OHLCV
        synthetic_csv = stress_dir / f"stress_{scenario}_hourly.csv"
        start_date = "2020-01-01"
        hours = 4320  # 180 days * 24 hours
        
        cmd = f"python3 src/regime.py --scenario {scenario} --output {synthetic_csv} --start-date {start_date} --hours {hours}"
        
        if not run_command(cmd, logger, f"Generate synthetic data for {scenario}"):
            logger.warning(f"Skipping scenario {scenario} due to generation failure")
            continue
        
        # Step 2: Generate signals
        signals_csv = stress_dir / f"stress_{scenario}_signals.csv"
        
        cmd = f"python3 src/signals.py --input {synthetic_csv} --config {config_path} --output {signals_csv}"
        
        if not run_command(cmd, logger, f"Generate signals for {scenario}"):
            logger.warning(f"Skipping scenario {scenario} due to signal generation failure")
            continue
        
        # Step 3: Run execution
        exec_output_dir = stress_dir / f"stress_{scenario}_execution"
        exec_output_dir.mkdir(exist_ok=True)
        
        cmd = f"python3 src/execution.py --hourly {synthetic_csv} --signals {signals_csv} --config {config_path} --output-dir {exec_output_dir}"
        
        if not run_command(cmd, logger, f"Run execution for {scenario}"):
            logger.warning(f"Skipping scenario {scenario} due to execution failure")
            continue
        
        # Step 4: Check trades generated
        trades_csv = exec_output_dir / "trades.csv"
        
        if not check_trades_generated(trades_csv, logger):
            logger.error(f"INTEGRATION FAILURE: Scenario {scenario} produced zero trades")
            logger.error(f"Stopping Stage 5 execution")
            sys.exit(1)
        
        # Step 5: Run validation
        ledger_csv = exec_output_dir / "capital_ledger.csv"
        audit_json = stress_dir / f"stress_{scenario}_audit_report.json"
        
        cmd = f"python3 src/validation.py --trades {trades_csv} --ledger {ledger_csv} --output {audit_json}"
        
        if not run_command(cmd, logger, f"Validate {scenario}"):
            logger.warning(f"Validation failed for scenario {scenario}")
        
        logger.info(f"Scenario {scenario} complete")
    
    # =====================================================================
    # PARAMETER SWEEP
    # =====================================================================
    
    logger.info("\n" + "=" * 70)
    logger.info("PARAMETER SWEEP")
    logger.info("=" * 70)
    
    sweep_dir = run_dir / "sweep_results"
    sweep_dir.mkdir(exist_ok=True)
    
    # Create grid JSON file
    grid_json = run_dir / "sweep_grid.json"
    import json
    with open(grid_json, 'w') as f:
        json.dump(sweep_grid, f, indent=2)
    
    logger.info(f"Sweep grid: {sweep_grid}")
    
    cmd = f"python3 src/sweeper.py --config {config_path} --grid {grid_json} --output-dir {sweep_dir}"
    
    if not run_command(cmd, logger, "Run parameter sweep"):
        logger.error("Parameter sweep failed")
        sys.exit(1)
    
    # Check sweep summary generated
    sweep_summary = sweep_dir / "sweep_summary.csv"
    if not sweep_summary.exists():
        logger.error(f"Sweep summary not found: {sweep_summary}")
        sys.exit(1)
    
    logger.info(f"Sweep summary generated: {sweep_summary}")
    
    # =====================================================================
    # VALIDATION OF BASELINE RUN
    # =====================================================================
    
    logger.info("\n" + "=" * 70)
    logger.info("BASELINE VALIDATION")
    logger.info("=" * 70)
    
    # Find latest Stage 4 run or run baseline execution
    results_dir = Path("results")
    stage4_runs = [d for d in results_dir.iterdir() if d.is_dir() and (d / "stage4").exists()]
    
    if stage4_runs:
        latest_stage4 = sorted(stage4_runs, key=lambda x: x.name)[-1] / "stage4"
        logger.info(f"Using existing Stage 4 run: {latest_stage4.parent.name}")
        
        baseline_trades = latest_stage4 / "trades.csv"
        baseline_ledger = latest_stage4 / "capital_ledger.csv"
    else:
        logger.info("No Stage 4 run found - generating baseline execution")
        
        baseline_dir = run_dir / "baseline_execution"
        baseline_dir.mkdir(exist_ok=True)
        
        # Generate baseline signals
        hourly_input = Path(config['data']['hourly_input_path'])
        baseline_signals = baseline_dir / "baseline_signals.csv"
        
        cmd = f"python3 src/signals.py --input {hourly_input} --config {config_path} --output {baseline_signals}"
        
        if not run_command(cmd, logger, "Generate baseline signals"):
            logger.error("Baseline signal generation failed")
            sys.exit(1)
        
        # Run baseline execution
        baseline_exec_dir = baseline_dir / "execution"
        baseline_exec_dir.mkdir(exist_ok=True)
        
        cmd = f"python3 src/execution.py --hourly {hourly_input} --signals {baseline_signals} --config {config_path} --output-dir {baseline_exec_dir}"
        
        if not run_command(cmd, logger, "Run baseline execution"):
            logger.error("Baseline execution failed")
            sys.exit(1)
        
        baseline_trades = baseline_exec_dir / "trades.csv"
        baseline_ledger = baseline_exec_dir / "capital_ledger.csv"
    
    # Validate baseline
    baseline_audit = run_dir / "baseline_audit_report.json"
    
    cmd = f"python3 src/validation.py --trades {baseline_trades} --ledger {baseline_ledger} --output {baseline_audit}"
    
    if not run_command(cmd, logger, "Validate baseline"):
        logger.error("Baseline validation failed")
        sys.exit(1)
    
    # Check baseline audit
    with open(baseline_audit, 'r') as f:
        audit_data = json.load(f)
    
    total_violations = audit_data.get('summary', {}).get('total_violations', 0)
    logger.info(f"Baseline validation complete: {total_violations} violations")
    
    # =====================================================================
    # SUMMARY
    # =====================================================================
    
    logger.info("\n" + "=" * 70)
    logger.info("STAGE 5 COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Output directory: {run_dir}")
    logger.info(f"Stress scenarios completed: {len(stress_scenarios)}")
    logger.info(f"Baseline violations: {total_violations}")
    logger.info(f"Sweep summary: {sweep_summary}")
    logger.info("=" * 70)
    
    # Create execution summary
    summary_path = run_dir / "execution_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("Stage 5 Execution Summary\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Run Directory: {run_dir}\n")
        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        f.write(f"Config: {config_path}\n\n")
        f.write(f"Stress Scenarios: {len(stress_scenarios)}\n")
        f.write(f"Baseline Violations: {total_violations}\n")
        f.write(f"Sweep Results: {sweep_summary}\n\n")
        f.write("All outputs are raw data only.\n")
        f.write("No analytical interpretation provided.\n")
    
    logger.info(f"Execution summary: {summary_path}")


if __name__ == "__main__":
    main()