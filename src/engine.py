import pandas as pd
import numpy as np
from typing import Dict, Any

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