import pandas as pd
import numpy as np
import json
import os
from pathlib import Path

def generate_synthetic_ohlcv(scenario_type, n_hours=1500, seed=42, base_price=10000.0):
    np.random.seed(seed)
    timestamps = pd.date_range(start="2017-10-20", periods=n_hours, freq='h')
    
    prices = [base_price]
    for i in range(1, n_hours):
        # Mechanical Cycle to force Entry and Exit
        if i % 24 == 0:
            prices.append(prices[-1] * 1.10) # Midnight Spike (High Prev Close)
        elif i % 24 == 1:
            prices.append(prices[-1] * 0.90) # Morning Dip (Entry Trigger)
        elif i % 24 == 12:
            prices.append(prices[-1] * 1.15) # Mid-day Rally (Activate TSL Trigger)
        elif i % 24 == 18:
            prices.append(prices[-1] * 0.85) # Evening Drop (Force TSL Exit)
        else:
            # Add scenario-specific stress at the midpoint
            change = np.random.normal(0, 0.005)
            if scenario_type == "flash_crash" and 600 < i < 610:
                change = -0.08
            elif scenario_type == "deep_bleed" and 600 < i < 900:
                change = -0.01
            prices.append(prices[-1] * (1 + change))

    df = pd.DataFrame({'Close': prices}, index=timestamps)
    df['Open'] = df['Close'].shift(1).fillna(base_price)
    df['High'] = df[['Open', 'Close']].max(axis=1) * 1.002
    df['Low'] = df[['Open', 'Close']].min(axis=1) * 0.998
    df['Volume'] = 1000.0
    
    # Internal Engine Indicators
    df['rsi'] = 25.0
    df['ma_value'] = df['Close'] * 0.95
    
    df.index.name = 'Datetime_Obj'
    df = df.reset_index()
    df['Open_Time'] = df['Datetime_Obj'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df

def create_synthetic_suite(base_path="."):
    data_dir = Path(base_path) / "data" / "synthetic"
    data_dir.mkdir(parents=True, exist_ok=True)
    scenarios = [
        {"name": "black_swan_flash_crash", "type": "flash_crash"},
        {"name": "black_swan_deep_bleed", "type": "deep_bleed"},
        {"name": "chop_tight_range", "type": "sideways"},
        {"name": "chop_wild_range", "type": "volatile"}
    ]
    manifest = []
    for sc in scenarios:
        filename = f"{sc['name']}.csv"
        df = generate_synthetic_ohlcv(sc['type'])
        df.to_csv(data_dir / filename, index=False)
        manifest.append({"scenario": sc['name'], "filename": filename})
    with open(data_dir / "synthetic_manifest.json", "w") as f:
        json.dump(manifest, f, indent=4)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Synthetic Data Generation CLI")
    
    # Required Arguments
    parser.add_argument("--scenario", type=str, required=True, help="flash_crash, deep_bleed, etc.")
    parser.add_argument("--output", type=str, required=True, help="Path to save hourly CSV")
    
    # Optional Arguments
    parser.add_argument("--hours", type=int, default=1500, help="Number of hours to generate")
    parser.add_argument("--base-price", type=float, default=10000.0, help="Starting price")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    # Invoke logic with parsed arguments
    df = generate_synthetic_ohlcv(
        scenario_type=args.scenario, 
        n_hours=args.hours, 
        base_price=args.base_price,
        seed=args.seed
    )
    
    df.to_csv(args.output, index=False)
    print(f"Generated {args.scenario} data to {args.output}")