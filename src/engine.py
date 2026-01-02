import pandas as pd
import numpy as np
from typing import Dict, Any, List

class Stage1Engine:
    """
    Research Engine Data Layer. 
    Handles deterministic daily signal generation from 1H input.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def aggregate_daily(self, df_1h: pd.DataFrame) -> pd.DataFrame:
        """Derives daily candles exclusively from 1H Datetime_Obj."""
        df_1h = df_1h.copy()
        df_1h['Date_Group'] = df_1h['Datetime_Obj'].dt.date
        
        daily = df_1h.groupby('Date_Group').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).reset_index()
        
        daily.rename(columns={'Date_Group': 'date'}, inplace=True)
        return daily

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates indicators on Daily close prices only."""
        # 1. Moving Average
        ma_p = self.config.get('ma_period', 20)
        if self.config.get('ma_type') == 'EMA':
            df['ma_value'] = df['Close'].ewm(span=ma_p, adjust=False).mean()
        else:
            df['ma_value'] = df['Close'].rolling(window=ma_p).mean()

        # 2. RSI (Standard 14-period Wilder's Smoothing)
        rsi_p = self.config.get('rsi_period', 14)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=rsi_p).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_p).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # 3. Peak / Drawdown Logic
        peak_w = self.config.get('peak_window', 252)
        df['peak'] = df['Close'].rolling(window=peak_w, min_periods=1).max()
        df['drawdown'] = (df['Close'] - df['peak']) / df['peak']
        
        return df

    def apply_signal_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Emits daily_signal based on configurable parameters.
        Logic is vectorized for deterministic backtesting.
        """
        rsi_min = self.config.get('rsi_threshold', 30)
        dd_min = self.config.get('dd_threshold', -0.10)
        
        # Rule: Permitted if Close > MA AND RSI > Limit AND Drawdown > Limit
        df['daily_signal'] = (
            (df['Close'] > df['ma_value']) & 
            (df['rsi'] > rsi_min) & 
            (df['drawdown'] > dd_min)
        )
        return df
    
class EntryEngine:
    """
    Stage 2: Hourly Entry Logic.
    Consumes Daily Signals and executes entries based on intra-day dips.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dip_pct = config.get("entry_dip_pct", 0.02)  # 2% dip default
        # Reference: 'close' or 'ma_value'
        self.ref_type = config.get("entry_ref_type", "close")

    def find_entries(self, df_1h: pd.DataFrame, df_daily: pd.DataFrame) -> pd.DataFrame:
        """
        Scans hourly data for entries following a permitted daily signal.
        """
        entries = []
        entry_id = 1
        
        # Merge daily signals into a dictionary for O(1) lookup
        # signal_date -> {ref_price, signal_bool}
        signal_lookup = df_daily.set_index('date').to_dict('index')

        # Identify unique dates in 1H data
        df_1h['date_key'] = df_1h['Datetime_Obj'].dt.date
        available_dates = df_1h['date_key'].unique()

        for date in available_dates:
            # We look for the signal from the PREVIOUS day to trade TODAY
            prev_date = date - pd.Timedelta(days=1)
            
            if prev_date not in signal_lookup:
                continue
                
            day_signal = signal_lookup[prev_date]
            
            # Condition 1: Daily Permission must be True
            if not day_signal.get('daily_signal', False):
                continue

            # Determine Reference Price for the dip
            # Case-insensitive mapping to Stage 1 output columns
            ref_price = day_signal.get('Close') if self.ref_type == "close" else day_signal.get('ma_value')
            
            if pd.isna(ref_price):
                continue

            target_price = ref_price * (1 - self.dip_pct)

            # Filter 1H candles for the current UTC day
            day_candles = df_1h[df_1h['date_key'] == date].sort_values('Open_Time')

            # Condition 2: Scan hourly candles for the first dip
            for _, candle in day_candles.iterrows():
                if candle['Low'] <= target_price:
                    # Entry Triggered
                    # Logic: If Open is already below target, we take Open. 
                    # Otherwise, we take the Target Price (Limit Fill).
                    if candle['Open'] <= target_price:
                        # Gap down: we get filled at the better (lower) Open price
                        exec_price = candle['Open']
                    else:
                        # Normal dip: we get filled exactly at our limit price
                        exec_price = target_price

                    entries.append({
                        "entry_id": entry_id,
                        "signal_date": prev_date,
                        "entry_open_time": int(candle['Open_Time']),
                        "entry_datetime": candle['Datetime_Obj'].strftime('%Y-%m-%d %H:%M:%S'),
                        "entry_price": exec_price,
                        "dip_pct": self.dip_pct,
                        "reference_price": ref_price,
                        "reference_type": self.ref_type
                    })
                    
                    entry_id += 1
                    # Finalization: Only one entry per daily signal
                    break 

        return pd.DataFrame(entries)