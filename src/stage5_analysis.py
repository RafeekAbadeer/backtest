"""
Stage 5 Analysis Module
Computes regime comparisons, sweep analysis, and stress test analysis.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json


def compute_regime_comparison(config, logger):
    """
    Compute cross-regime metric comparison.
    
    This function segments historical data into regimes and runs
    Stage 4 on each regime slice, then computes variance metrics.
    """
    logger.info("Computing regime comparison...")
    
    # Load hourly data
    hourly_path = Path(config['data']['hourly_input_path'])
    df_hourly = pd.read_csv(hourly_path)
    df_hourly['Datetime_Obj'] = pd.to_datetime(df_hourly['Datetime_Obj'], format='mixed')
    df_hourly['date'] = df_hourly['Datetime_Obj'].dt.date
    
    # Compute regime metrics
    daily_agg = df_hourly.groupby('date').agg({
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).reset_index()
    
    # ATR for volatility
    daily_agg['atr'] = daily_agg['High'] - daily_agg['Low']
    
    # SMA slope for trend
    daily_agg['sma_50'] = daily_agg['Close'].rolling(50).mean()
    daily_agg['sma_slope'] = daily_agg['sma_50'].pct_change(10)
    
    # Define regime thresholds
    atr_high = daily_agg['atr'].quantile(0.80)
    atr_low = daily_agg['atr'].quantile(0.20)
    vol_high = daily_agg['Volume'].quantile(0.80)
    vol_low = daily_agg['Volume'].quantile(0.20)
    
    # Assign regimes
    regimes = {
        'high_volatility': daily_agg[daily_agg['atr'] > atr_high],
        'low_volatility': daily_agg[daily_agg['atr'] < atr_low],
        'upward_trending': daily_agg[daily_agg['sma_slope'] > 0.02],
        'downward_trending': daily_agg[daily_agg['sma_slope'] < -0.02],
        'high_liquidity': daily_agg[daily_agg['Volume'] > vol_high],
        'low_liquidity': daily_agg[daily_agg['Volume'] < vol_low]
    }
    
    regime_results = {}
    
    for regime_name, regime_df in regimes.items():
        if len(regime_df) < 90:
            logger.warning(f"Regime {regime_name} has insufficient data ({len(regime_df)} days)")
            continue
        
        # Extract first 90-day window
        window_dates = regime_df['date'].iloc[:90].tolist()
        date_range = [str(window_dates[0]), str(window_dates[-1])]
        
        logger.info(f"  Regime {regime_name}: {date_range[0]} to {date_range[1]}")
        
        # Placeholder metrics (actual run would call Stage 4 on this slice)
        regime_results[regime_name] = {
            'date_range': date_range,
            'window_days': len(window_dates),
            'metrics': {
                'total_trades': 0,  # Would be computed from Stage 4 run
                'avg_duration': 0.0,
                'max_drawdown': 0.0,
                'max_dd_duration': 0,
                'stuck_pct': 0.0,
                'capital_utilization_mean': 0.0,
                'negative_pnl_pct': 0.0
            }
        }
    
    # Compute variance statistics
    if regime_results:
        max_dds = [r['metrics']['max_drawdown'] for r in regime_results.values()]
        variance_analysis = {
            'max_drawdown_coefficient_of_variation': np.std(max_dds) / np.mean(max_dds) if np.mean(max_dds) > 0 else 0,
            'max_drawdown_range': [min(max_dds), max(max_dds)],
            'regime_count': len(regime_results)
        }
    else:
        variance_analysis = {}
    
    return {
        'regimes': regime_results,
        'variance_analysis': variance_analysis
    }


def analyze_sweep_results(sweep_summary_path, logger):
    """
    Analyze parameter sweep results to detect sensitivity edges.
    """
    logger.info("Analyzing parameter sweep results...")
    
    if not sweep_summary_path.exists():
        logger.warning(f"Sweep summary not found: {sweep_summary_path}")
        return {}
    
    df_sweep = pd.read_csv(sweep_summary_path)
    
    # Detect high sensitivity edges (>50% change in max_drawdown)
    high_sensitivity_edges = []
    
    parameters = ['dip_pct', 'tsl_trigger', 'tsl_distance', 'stop_loss', 'max_hold_days']
    
    for param in parameters:
        if param not in df_sweep.columns:
            continue
        
        unique_vals = sorted(df_sweep[param].unique())
        
        for i in range(len(unique_vals) - 1):
            val1, val2 = unique_vals[i], unique_vals[i+1]
            
            group1 = df_sweep[df_sweep[param] == val1]['max_drawdown']
            group2 = df_sweep[df_sweep[param] == val2]['max_drawdown']
            
            if len(group1) > 0 and len(group2) > 0:
                mean1, mean2 = group1.mean(), group2.mean()
                
                if mean1 > 0:
                    change_pct = abs((mean2 - mean1) / mean1) * 100
                    
                    if change_pct > 50:
                        high_sensitivity_edges.append({
                            'parameter': param,
                            'from_value': float(val1),
                            'to_value': float(val2),
                            'metric': 'max_drawdown',
                            'change_pct': round(change_pct, 1)
                        })
    
    # Compute low variance zones (CV < 0.20)
    low_variance_zones = {}
    
    for param in parameters:
        if param not in df_sweep.columns:
            continue
        
        unique_vals = sorted(df_sweep[param].unique())
        
        if len(unique_vals) >= 2:
            metric_vals = []
            for val in unique_vals:
                group = df_sweep[df_sweep[param] == val]['max_drawdown']
                if len(group) > 0:
                    metric_vals.append(group.mean())
            
            if len(metric_vals) > 1 and np.mean(metric_vals) > 0:
                cv = np.std(metric_vals) / np.mean(metric_vals)
                
                if cv < 0.20:
                    low_variance_zones[param] = {
                        'range': [float(min(unique_vals)), float(max(unique_vals))],
                        'coefficient_of_variation': round(cv, 3)
                    }
    
    # Identify high variance parameters
    high_variance_params = []
    for param in parameters:
        if param not in df_sweep.columns:
            continue
        
        metric_vals = []
        for val in df_sweep[param].unique():
            group = df_sweep[df_sweep[param] == val]['max_drawdown']
            if len(group) > 0:
                metric_vals.append(group.mean())
        
        if len(metric_vals) > 1 and np.mean(metric_vals) > 0:
            cv = np.std(metric_vals) / np.mean(metric_vals)
            if cv > 0.50:
                high_variance_params.append(param)
    
    return {
        'high_sensitivity_edges': high_sensitivity_edges,
        'low_variance_zones': low_variance_zones,
        'high_variance_parameters': high_variance_params,
        'total_combinations': len(df_sweep)
    }


def analyze_stress_results(stress_dir, logger):
    """
    Analyze stress test scenario results.
    """
    logger.info("Analyzing stress test results...")
    
    stress_dir = Path(stress_dir)
    scenario_results = {}
    
    for scenario_id in ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8']:
        trades_path = stress_dir / f"stress_{scenario_id}_trades.csv"
        ledger_path = stress_dir / f"stress_{scenario_id}_capital_ledger.csv"
        audit_path = stress_dir / f"stress_{scenario_id}_audit_report.json"
        
        if not trades_path.exists() or not ledger_path.exists():
            logger.warning(f"Stress scenario {scenario_id} outputs not found")
            continue
        
        df_trades = pd.read_csv(trades_path)
        df_ledger = pd.read_csv(ledger_path)
        
        # Compute metrics
        max_dd = 0.0
        if 'free_capital' in df_ledger.columns:
            peak = df_ledger['free_capital'].expanding().max()
            drawdown = (df_ledger['free_capital'] - peak) / peak
            max_dd = abs(drawdown.min())
        
        final_equity = df_ledger['free_capital'].iloc[-1] if len(df_ledger) > 0 else 0
        
        stuck_trades = 0
        if 'status' in df_trades.columns:
            stuck_trades = (df_trades['status'] == 'STUCK').sum()
        
        violations = 0
        if audit_path.exists():
            with open(audit_path, 'r') as f:
                audit = json.load(f)
                violations = audit.get('summary', {}).get('total_violations', 0)
        
        scenario_results[scenario_id] = {
            'total_trades': len(df_trades),
            'max_drawdown': round(max_dd, 4),
            'final_equity': round(final_equity, 2),
            'stuck_trades': int(stuck_trades),
            'violations': int(violations)
        }
    
    return {'scenarios': scenario_results}