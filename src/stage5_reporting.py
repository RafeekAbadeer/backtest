"""
Stage 5 Reporting Module
Generates static HTML reports with embedded charts.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
from pathlib import Path


def generate_html_report(report_data, output_path, logger):
    """
    Generate static HTML report with embedded charts.
    """
    logger.info("Generating HTML report...")
    
    html_parts = []
    
    # Header
    html_parts.append("""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Stage 5 Analysis Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 40px; }
        h1 { color: #333; border-bottom: 3px solid #007bff; padding-bottom: 10px; }
        h2 { color: #555; margin-top: 40px; border-bottom: 2px solid #ddd; padding-bottom: 8px; }
        h3 { color: #666; margin-top: 30px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #007bff; color: white; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        .violation { color: red; font-weight: bold; }
        .metric { font-family: monospace; }
        .summary { background: #e7f3ff; padding: 20px; border-left: 4px solid #007bff; margin: 20px 0; }
        img { max-width: 100%; height: auto; margin: 20px 0; }
    </style>
</head>
<body>
<div class="container">
""")
    
    # Title
    html_parts.append(f"""
<h1>Stage 5 Analysis Report</h1>
<div class="summary">
    <strong>Stage 4 Run:</strong> {report_data['run_id']}<br>
    <strong>Stage 5 Run:</strong> {report_data['stage5_run_id']}<br>
    <strong>Analysis Timestamp:</strong> {report_data['timestamp']}
</div>
""")
    
    # Section 1: Validation Results
    html_parts.append("<h2>1. Validation Results</h2>")
    
    audit = report_data['audit_report']
    total_violations = audit['summary']['total_violations']
    
    if total_violations == 0:
        html_parts.append('<p style="color: green; font-weight: bold;">✓ No violations detected</p>')
    else:
        html_parts.append(f'<p class="violation">✗ {total_violations} violations detected</p>')
    
    html_parts.append("<h3>Invariant Checks</h3>")
    html_parts.append("<table><tr><th>Invariant</th><th>Violation Count</th></tr>")
    
    for invariant, data in audit['violations'].items():
        count = data.get('count', 0)
        color = 'red' if count > 0 else 'inherit'
        html_parts.append(f"<tr><td>{invariant}</td><td style='color: {color}'>{count}</td></tr>")
    
    html_parts.append("</table>")
    
    # Section 2: Stress Test Results
    html_parts.append("<h2>2. Stress Test Results</h2>")
    
    stress = report_data['stress_analysis']
    
    if 'scenarios' in stress:
        html_parts.append("<table>")
        html_parts.append("<tr><th>Scenario</th><th>Total Trades</th><th>Max Drawdown</th><th>Final Equity</th><th>Stuck Trades</th><th>Violations</th></tr>")
        
        for scenario_id, metrics in stress['scenarios'].items():
            html_parts.append(f"""
<tr>
    <td>{scenario_id}</td>
    <td>{metrics['total_trades']}</td>
    <td class="metric">{metrics['max_drawdown']:.2%}</td>
    <td class="metric">${metrics['final_equity']:,.2f}</td>
    <td>{metrics['stuck_trades']}</td>
    <td style="color: {'red' if metrics['violations'] > 0 else 'inherit'}">{metrics['violations']}</td>
</tr>
""")
        
        html_parts.append("</table>")
    
    # Section 3: Regime Analysis
    html_parts.append("<h2>3. Regime Analysis</h2>")
    
    regime = report_data['regime_comparison']
    
    if 'variance_analysis' in regime:
        variance = regime['variance_analysis']
        html_parts.append(f"""
<p><strong>Max Drawdown Coefficient of Variation:</strong> <span class="metric">{variance.get('max_drawdown_coefficient_of_variation', 0):.3f}</span></p>
<p><strong>Regimes Analyzed:</strong> {variance.get('regime_count', 0)}</p>
""")
    
    # Section 4: Parameter Sweep Analysis
    html_parts.append("<h2>4. Parameter Sweep Analysis</h2>")
    
    sweep = report_data['sweep_analysis']
    
    html_parts.append(f"<p><strong>Total Combinations:</strong> {sweep.get('total_combinations', 0)}</p>")
    
    if 'high_sensitivity_edges' in sweep and sweep['high_sensitivity_edges']:
        html_parts.append("<h3>High Sensitivity Edges Detected</h3>")
        html_parts.append("<table><tr><th>Parameter</th><th>From Value</th><th>To Value</th><th>Metric</th><th>Change %</th></tr>")
        
        for edge in sweep['high_sensitivity_edges']:
            html_parts.append(f"""
<tr>
    <td>{edge['parameter']}</td>
    <td class="metric">{edge['from_value']}</td>
    <td class="metric">{edge['to_value']}</td>
    <td>{edge['metric']}</td>
    <td class="violation">{edge['change_pct']:.1f}%</td>
</tr>
""")
        
        html_parts.append("</table>")
    else:
        html_parts.append("<p>No high sensitivity edges detected.</p>")
    
    if 'low_variance_zones' in sweep and sweep['low_variance_zones']:
        html_parts.append("<h3>Low Variance Zones</h3>")
        html_parts.append("<table><tr><th>Parameter</th><th>Range</th><th>Coefficient of Variation</th></tr>")
        
        for param, data in sweep['low_variance_zones'].items():
            html_parts.append(f"""
<tr>
    <td>{param}</td>
    <td class="metric">[{data['range'][0]}, {data['range'][1]}]</td>
    <td class="metric">{data['coefficient_of_variation']:.3f}</td>
</tr>
""")
        
        html_parts.append("</table>")
    
    # Footer
    html_parts.append("""
</div>
</body>
</html>
""")
    
    # Write HTML
    with open(output_path, 'w') as f:
        f.write(''.join(html_parts))
    
    logger.info(f"Report written to: {output_path}")