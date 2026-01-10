import pandas as pd
import matplotlib.pyplot as plt
import base64
from io import BytesIO
from pathlib import Path

def generate_audit_visuals(ledger_path: str, audit_results: dict, output_dir: str):
    """
    Generates a static HTML report for human inspection of capital accounting.
    Strictly diagnostic: Equity, Exposure, and Drawdown Durations only.
    """
    df = pd.read_csv(ledger_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Calculate deterministic diagnostic curves
    df['total_equity'] = df['free_capital'] + df['total_exposure']
    df['equity_peak'] = df['total_equity'].cummax()
    df['underwater'] = df['total_equity'] < df['equity_peak']
    
    # Create static Plot (Matplotlib)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    # Chart 1: Capital Continuity & Exposure
    ax1.fill_between(df['timestamp'], 0, df['total_equity'], color='lightgrey', label='Total Equity (Accounting Sum)')
    ax1.fill_between(df['timestamp'], 0, df['total_exposure'], color='orange', alpha=0.5, label='Exposed Capital')
    ax1.step(df['timestamp'], df['free_capital'], where='post', color='blue', linewidth=1, label='Free Capital')
    ax1.set_ylabel('Capital Units')
    ax1.set_title('Diagnostic 1: Capital Allocation & Exposure Continuity')
    ax1.legend(loc='upper left')
    ax1.grid(True, which='both', linestyle='--', alpha=0.5)

    # Chart 2: Drawdown Duration & Depth (Fragility Signal)
    dd_pct = (df['total_equity'] - df['equity_peak']) / df['equity_peak']
    ax2.fill_between(df['timestamp'], 0, dd_pct, color='red', alpha=0.3, label='Drawdown Depth')
    ax2.set_ylabel('Drawdown %')
    ax2.set_title('Diagnostic 2: Drawdown Depth and Duration (Fragility)')
    ax2.grid(True, which='both', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    
    # Convert plot to base64 for static HTML inclusion
    tmpfile = BytesIO()
    fig.savefig(tmpfile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
    plt.close(fig)

    # Build Static HTML
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Audit Visuals - Diagnostic Report</title>
        <style>
            body {{ font-family: monospace; margin: 40px; line-height: 1.6; color: #333; }}
            .container {{ max-width: 1200px; margin: auto; }}
            .audit-box {{ border: 2px solid #333; padding: 20px; margin-bottom: 30px; }}
            .fail {{ color: #a00; font-weight: bold; }}
            .pass {{ color: #0a0; font-weight: bold; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f4f4f4; }}
            img {{ max-width: 100%; height: auto; border: 1px solid #ccc; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Stage 4 Execution Audit: Diagnostic Inspection</h1>
            
            <div class="audit-box">
                <h2>Machine Verdict: { "FAIL" if audit_results['failures'] else "PASS" }</h2>
                <h3>Audit Log:</h3>
                <ul>
                    {"".join([f"<li>{err}</li>" for err in audit_results.get('failures', [])]) if audit_results.get('failures') else "<li>No invariant violations detected.</li>"}
                </ul>
            </div>

            <h2>Deterministic Equity & Exposure Curves</h2>
            <img src="data:image/png;base64,{encoded}" alt="Accounting Charts">

            <h2>Accounting Statistics (Post-Execution)</h2>
            <table>
                <tr><th>Metric Label</th><th>Value</th></tr>
                <tr><td>Max Drawdown Depth Observed</td><td>{audit_results['accounting_integrity'].get('max_drawdown_depth', 0):.4f}</td></tr>
                <tr><td>Max Drawdown Duration (Hours)</td><td>{audit_results['accounting_integrity'].get('max_drawdown_duration_hours', 0)}</td></tr>
                <tr><td>Negative Free Capital Events</td><td>{audit_results['accounting_integrity'].get('negative_free_capital_events', 0)}</td></tr>
                <tr><td>Off-Schedule Injections</td><td>{audit_results['replenishment_audit'].get('off_schedule_injections', 0)}</td></tr>
                <tr><td>Broken Pro-Rata Invariants</td><td>{audit_results['trade_consistency'].get('broken_capital_invariants', 0)}</td></tr>
            </table>
            
            <p><i>Note: This report is a literal representation of ledger data. No performance ranking or optimization is implied.</i></p>
        </div>
    </body>
    </html>
    """

    output_path = Path(output_dir) / "audit_visuals.html"
    with open(output_path, "w") as f:
        f.write(html_content)