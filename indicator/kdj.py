import backtrader as bt
import pandas as pd
import numpy as np
from .ema_adjust import ExponentialMovingAverageAdjust

class KDJ(bt.Indicator):
    '''
    This generic indicator doesn't assume the data feed has the components
    ``high``, ``low`` and ``close``. It needs three data sources passed to it,
    which whill considered in that order. (following the OHLC standard naming)
    '''
    lines = ('j',)
    params = dict(
        pk=9,
        pd=3,
        pdslow=3,
        mv=ExponentialMovingAverageAdjust,
        slowav=None,
    )

    def __init__(self):
        # Get highest from period k from 1st data
        highest = bt.ind.Highest(self.data0.high, period=self.p.pk)
        # Get lowest from period k from 2nd data
        lowest = bt.ind.Lowest(self.data0.low, period=self.p.pk)

        # Apply the formula to get raw K
        RSV = 100.0 * (self.data0.close - lowest) / (highest - lowest)

        # The standard k in the indicator is a smoothed versin of K
        k = self.p.mv(RSV, period=self.p.pd)

        # Smooth k => d
        d = self.p.mv(k, period=self.p.pdslow)

        self.l.j = 3. * k - 2. * d


class KDJPandas:
    """
    Pandas-compatible KDJ indicator that can be used directly with pandas DataFrames.
    This class provides the same calculation logic as the backtrader KDJ indicator.
    """
    
    def __init__(self, pk: int = 9, pd: int = 3, pdslow: int = 3):
        """
        Initialize KDJ calculator.
        
        Args:
            pk (int): Period for K calculation (default: 9)
            pd (int): Period for D calculation (default: 3)
            pdslow (int): Period for slow D calculation (default: 3)
        """
        self.pk = pk
        self.pd = pd
        self.pdslow = pdslow
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate KDJ indicators for a pandas DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame with OHLCV data (must have 'high', 'low', 'close' columns)
            
        Returns:
            pd.DataFrame: DataFrame with K, D, J columns added
        """
        # Ensure we have the required columns
        required_cols = ['high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Missing required columns. Need: {required_cols}, have: {list(df.columns)}")
        
        df = df.copy()
        
        # Get highest high and lowest low over pk period (same as backtrader KDJ)
        highest = df['high'].rolling(window=self.pk).max()
        lowest = df['low'].rolling(window=self.pk).min()
        
        # Calculate RSV (Raw Stochastic Value) - same formula as backtrader KDJ
        rsv = 100.0 * (df['close'] - lowest) / (highest - lowest)
        
        # Calculate K using EMA (same as backtrader KDJ)
        # Overwrite alpha
        alpha = 1.0 / self.pd
        k = rsv.ewm(alpha=alpha, adjust=False).mean()
        
        # Calculate D using EMA (same as backtrader KDJ)
        d = k.ewm(alpha=alpha, adjust=False).mean()
        
        # Calculate J (same formula as backtrader KDJ)
        j = 3.0 * k - 2.0 * d
        
        # Add KDJ columns to DataFrame
        df['k'] = k
        df['d'] = d
        df['j'] = j
        
        return df
    
    def get_latest(self, df: pd.DataFrame) -> tuple:
        """
        Get the latest K, D, J values.
        
        Args:
            df (pd.DataFrame): DataFrame with KDJ columns
            
        Returns:
            tuple: Latest (K, D, J) values
        """
        if 'k' not in df.columns or 'd' not in df.columns or 'j' not in df.columns:
            raise ValueError("DataFrame must have KDJ columns. Call calculate() first.")
        
        latest_k = df['k'].iloc[-1]
        latest_d = df['d'].iloc[-1]
        latest_j = df['j'].iloc[-1]
        
        return latest_k, latest_d, latest_j
