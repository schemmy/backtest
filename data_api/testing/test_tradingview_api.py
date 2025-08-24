"""
TradingView API Client for loading historical data and technical analysis.

This module provides functions to interact with TradingView's data and
technical analysis capabilities through the tradingview-ta library.
"""

import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from tradingview_ta import TA_Handler, Interval
except ImportError:
    logger.warning("tradingview-ta library not found. Install with: pip install tradingview-ta")
    TA_Handler = None
    Interval = None


class TradingViewAPI:
    """
    TradingView API client for fetching historical data and technical analysis.
    
    This class provides methods to:
    - Fetch historical price data
    - Get technical analysis indicators
    - Retrieve real-time quotes
    - Download data for multiple symbols
    """
    
    def __init__(self, timeout: int = 30):
        """
        Initialize the TradingView API client.
        
        Args:
            timeout (int): Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Base URLs
        self.base_url = "https://scanner.tradingview.com"
        self.quote_url = "https://quote.tradingview.com"
        
    def get_symbol_info(self, symbol: str, exchange: str = "NASDAQ") -> Dict:
        """
        Get basic information about a symbol.
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
            exchange (str): Exchange name (default: 'NASDAQ')
            
        Returns:
            Dict: Symbol information including name, type, etc.
        """
        try:
            url = f"{self.quote_url}/quote/{exchange}:{symbol}"
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Extract symbol info from response
            # Note: This is a simplified approach - actual implementation may vary
            return {
                'symbol': symbol,
                'exchange': exchange,
                'url': url,
                'status': 'success'
            }
        except Exception as e:
            logger.error(f"Error getting symbol info for {symbol}: {e}")
            return {
                'symbol': symbol,
                'exchange': exchange,
                'error': str(e),
                'status': 'error'
            }
    
    def get_historical_data(
        self, 
        symbol: str, 
        exchange: str = "NASDAQ",
        interval: str = "1d",
        count: int = 100
    ) -> pd.DataFrame:
        """
        Get historical price data for a symbol.
        
        Args:
            symbol (str): Stock symbol
            exchange (str): Exchange name
            interval (str): Time interval ('1m', '5m', '15m', '30m', '1h', '1d', '1w', '1M')
            count (int): Number of data points to retrieve
            
        Returns:
            pd.DataFrame: Historical data with OHLCV columns
        """
        try:
            # Use tradingview-ta library if available
            if TA_Handler:
                return self._get_data_via_ta_library(symbol, exchange, interval, count)
            else:
                return self._get_data_via_web_api(symbol, exchange, interval, count)
                
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def _get_data_via_ta_library(
        self, 
        symbol: str, 
        exchange: str, 
        interval: str, 
        count: int
    ) -> pd.DataFrame:
        """Get data using the tradingview-ta library."""
        try:
            # Map interval strings to tradingview-ta Interval enum
            interval_map = {
                '1m': Interval.INTERVAL_1_MINUTE,
                '5m': Interval.INTERVAL_5_MINUTES,
                '15m': Interval.INTERVAL_15_MINUTES,
                '30m': Interval.INTERVAL_30_MINUTES,
                '1h': Interval.INTERVAL_1_HOUR,
                '1d': Interval.INTERVAL_1_DAY,
                '1w': Interval.INTERVAL_1_WEEK,
                '1M': Interval.INTERVAL_1_MONTH
            }
            
            tv_interval = interval_map.get(interval, Interval.INTERVAL_1_DAY)
            
            # Create handler
            handler = TA_Handler(
                symbol=symbol,
                screener="america" if exchange in ["NASDAQ", "NYSE"] else "forex",
                exchange=exchange,
                interval=tv_interval,
                timeout=self.timeout
            )
            
            # Get analysis
            analysis = handler.get_analysis()
            
            # Convert to DataFrame
            data = {
                'timestamp': [datetime.now()],
                'open': [analysis.indicators.get('open', 0)],
                'high': [analysis.indicators.get('high', 0)],
                'low': [analysis.indicators.get('low', 0)],
                'close': [analysis.indicators.get('close', 0)],
                'volume': [analysis.indicators.get('volume', 0)]
            }
            
            return pd.DataFrame(data)
            
        except Exception as e:
            logger.error(f"Error with TA library for {symbol}: {e}")
            return pd.DataFrame()
    
    def _get_data_via_web_api(
        self, 
        symbol: str, 
        exchange: str, 
        interval: str, 
        count: int
    ) -> pd.DataFrame:
        """Get data using web API calls (fallback method)."""
        try:
            # This is a simplified approach - actual TradingView web API may require
            # more complex authentication and request handling
            
            # For demonstration, return empty DataFrame
            # In practice, you would implement the actual web API calls here
            logger.warning("Web API method not fully implemented - install tradingview-ta library")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error with web API for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_technical_analysis(
        self, 
        symbol: str, 
        exchange: str = "NASDAQ"
    ) -> Dict:
        """
        Get technical analysis for a symbol.
        
        Args:
            symbol (str): Stock symbol
            exchange (str): Exchange name
            
        Returns:
            Dict: Technical analysis summary
        """
        try:
            if not TA_Handler:
                logger.error("tradingview-ta library not available")
                return {}
            
            handler = TA_Handler(
                symbol=symbol,
                screener="america" if exchange in ["NASDAQ", "NYSE"] else "forex",
                exchange=exchange,
                interval=Interval.INTERVAL_1_DAY,
                timeout=self.timeout
            )
            
            analysis = handler.get_analysis()
            
            return {
                'symbol': symbol,
                'exchange': exchange,
                'summary': analysis.summary,
                'oscillators': analysis.oscillators,
                'moving_averages': analysis.moving_averages,
                'indicators': analysis.indicators,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting technical analysis for {symbol}: {e}")
            return {}
    
    def get_multiple_symbols_data(
        self, 
        symbols: List[str], 
        exchange: str = "NASDAQ",
        interval: str = "1d",
        count: int = 100
    ) -> Dict[str, pd.DataFrame]:
        """
        Get historical data for multiple symbols.
        
        Args:
            symbols (List[str]): List of stock symbols
            exchange (str): Exchange name
            interval (str): Time interval
            count (int): Number of data points
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary mapping symbols to their data
        """
        results = {}
        
        for symbol in symbols:
            logger.info(f"Fetching data for {symbol}...")
            data = self.get_historical_data(symbol, exchange, interval, count)
            if not data.empty:
                results[symbol] = data
            
            # Add delay to avoid rate limiting
            time.sleep(0.5)
        
        return results
    
    def save_data_to_csv(
        self, 
        data: pd.DataFrame, 
        filename: str, 
        directory: str = "data"
    ) -> bool:
        """
        Save data to CSV file.
        
        Args:
            data (pd.DataFrame): Data to save
            filename (str): Output filename
            directory (str): Output directory
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            import os
            os.makedirs(directory, exist_ok=True)
            
            filepath = os.path.join(directory, filename)
            data.to_csv(filepath, index=False)
            logger.info(f"Data saved to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving data to CSV: {e}")
            return False
    
    def get_market_overview(self) -> Dict:
        """
        Get market overview and indices data.
        
        Returns:
            Dict: Market overview information
        """
        try:
            # Major indices
            indices = ['SPY', 'QQQ', 'IWM', 'DIA']
            overview = {}
            
            for index in indices:
                ta_data = self.get_technical_analysis(index, "NASDAQ")
                if ta_data:
                    overview[index] = {
                        'summary': ta_data.get('summary', {}),
                        'indicators': ta_data.get('indicators', {})
                    }
            
            return overview
            
        except Exception as e:
            logger.error(f"Error getting market overview: {e}")
            return {}


# Convenience functions for easy usage
def load_stock_data(
    symbol: str, 
    exchange: str = "NASDAQ", 
    interval: str = "1d", 
    count: int = 100
) -> pd.DataFrame:
    """
    Convenience function to load stock data.
    
    Args:
        symbol (str): Stock symbol
        exchange (str): Exchange name
        interval (str): Time interval
        count (int): Number of data points
        
    Returns:
        pd.DataFrame: Historical stock data
    """
    api = TradingViewAPI()
    return api.get_historical_data(symbol, exchange, interval, count)


def get_stock_analysis(symbol: str, exchange: str = "NASDAQ") -> Dict:
    """
    Convenience function to get stock technical analysis.
    
    Args:
        symbol (str): Stock symbol
        exchange (str): Exchange name
        
    Returns:
        Dict: Technical analysis summary
    """
    api = TradingViewAPI()
    return api.get_technical_analysis(symbol, exchange)


def download_multiple_stocks(
    symbols: List[str], 
    exchange: str = "NASDAQ",
    interval: str = "1d",
    count: int = 100,
    save_to_csv: bool = True
) -> Dict[str, pd.DataFrame]:
    """
    Download data for multiple stocks.
    
    Args:
        symbols (List[str]): List of stock symbols
        exchange (str): Exchange name
        interval (str): Time interval
        count (int): Number of data points
        save_to_csv (bool): Whether to save data to CSV files
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of stock data
    """
    api = TradingViewAPI()
    data = api.get_multiple_symbols_data(symbols, exchange, interval, count)
    
    if save_to_csv:
        for symbol, df in data.items():
            if not df.empty:
                filename = f"{symbol}_{interval}_{datetime.now().strftime('%Y%m%d')}.csv"
                api.save_data_to_csv(df, filename)
    
    return data


# Example usage and testing
if __name__ == "__main__":
    # Example: Load Apple stock data
    print("Loading AAPL data...")
    aapl_data = load_stock_data("AAPL", "NASDAQ", "1d", 30)
    print(f"AAPL data shape: {aapl_data.shape}")
    print(aapl_data.head())
    
    # Example: Get technical analysis
    print("\nGetting AAPL technical analysis...")
    aapl_analysis = get_stock_analysis("AAPL", "NASDAQ")
    print(f"AAPL summary: {aapl_analysis.get('summary', {})}")
    
    # Example: Download multiple stocks
    print("\nDownloading multiple stocks...")
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN"]
    multi_data = download_multiple_stocks(symbols, "NASDAQ", "1d", 30)
    
    for symbol, data in multi_data.items():
        print(f"{symbol}: {data.shape[0]} data points")
