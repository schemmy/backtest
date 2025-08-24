#!/usr/bin/env python3
"""
Stock Picker based on KDJ Indicator

This script loops through all stock data files in data/stock_data/a/ and
selects stocks where the most recent day's KDJ J indicator is less than 0.
"""

import pandas as pd
import numpy as np
import os
import glob
import sys
from datetime import datetime
import logging
from typing import List, Dict, Tuple

# Add parent directory to Python path to access indicator module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicator.kdj import KDJPandas

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockPicker:
    """Main class for picking stocks based on KDJ indicators."""
    
    def __init__(self, data_dir: str = "data/stock_data/"):
        """
        Initialize stock picker.
        
        Args:
            data_dir (str): Directory containing stock data files
        """
        self.data_dir = data_dir
        self.kdj_calculator = KDJPandas()
        self.results = []
        
        # Ensure data directory exists
        if not os.path.exists(data_dir):
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    def get_stock_files(self) -> List[str]:
        """Get list of all stock CSV files in the data directory."""
        pattern = os.path.join(self.data_dir + 'us/', "*.csv")
        stock_files_us = glob.glob(pattern)
        logger.info(f"Found {len(stock_files_us)} us stock files in {self.data_dir}")

        pattern = os.path.join(self.data_dir + 'a/', "*.csv")
        stock_files_a = glob.glob(pattern)
        logger.info(f"Found {len(stock_files_us)} cn stock files in {self.data_dir}")

        return stock_files_us + stock_files_a
    
    def process_stock_file(self, file_path: str) -> Dict:
        """
        Process a single stock file and calculate KDJ indicators.
        
        Args:
            file_path (str): Path to the stock CSV file
            
        Returns:
            Dict: Stock information with KDJ values
        """
        try:
            # Extract stock symbol from filename
            filename = os.path.basename(file_path)
            symbol = filename.replace('.csv', '')
            
            # Read stock data
            df = pd.read_csv(file_path)
            
            # Ensure we have enough data for KDJ calculation
            if len(df) < 20:  # Need at least 20 data points for reliable KDJ
                logger.info(f"{symbol}: Insufficient data points ({len(df)})")
                return None
            
            # Calculate KDJ indicators using the indicator module
            df_with_kdj = self.kdj_calculator.calculate(df)
            # Get latest KDJ values
            latest_k, latest_d, latest_j = self.kdj_calculator.get_latest(df_with_kdj)
            
            # Get latest price data
            latest_close = df['close'].iloc[-1]
            latest_date = df['date'].iloc[-1]

            lastest_turnover_mv5 = df['volume'].ewm(span=5, adjust=False).mean().iloc[-1]
            
            # Create stock info
            stock_info = {
                'symbol': symbol,
                'file_path': file_path,
                'latest_date': latest_date,
                'latest_close': latest_close,
                'turnover_mv5': round(lastest_turnover_mv5, 3),
                'k_value': round(latest_k, 3),
                'd_value': round(latest_d, 3),
                'j_value': round(latest_j, 3),
                'data_points': len(df),
                'j_less_than_zero': latest_j < 0,
                
            }
            
            return stock_info
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return None
    
    def pick_stocks(self) -> List[Dict]:
        """
        Pick stocks where J value is less than the threshold.
        
        Args:
            j_threshold (float): J value threshold (default: 0.0)
            
        Returns:
            List[Dict]: List of stocks meeting the criteria
        """
        logger.info(f"Starting stock picking process with J threshold")
        
        stock_files = sorted(self.get_stock_files())
        selected_stocks = []
        
        for i, file_path in enumerate(stock_files):
            logger.info(f"Processing {i+1}/{len(stock_files)}: {os.path.basename(file_path)}")
            
            stock_info = self.process_stock_file(file_path)
            
            # Condition 1: Turnover 
            turnover_threshold = 1e6
            if stock_info['symbol'][0].isdigit():
                # CN stock turnover >= 100M
                turnover_threshold = 1e8
            else:
                # US stock turnover >= 10M
                turnover_threshold = 1e7
            condition_1 = stock_info['turnover_mv5'] >= turnover_threshold

            # Condition 2:Check if J value meets criteria
            j_threshold = 0.0
            if stock_info['turnover_mv5'] >= 1e9:
                # If turnover >= 1B, j_threshold = 8, else 0
                j_threshold = 8
            condition_2 = stock_info['j_value'] <= j_threshold

            if stock_info is not None:
                if condition_1 and condition_2:
                    selected_stocks.append(stock_info)
                    logger.warning(f"""✅ {stock_info['symbol']}: J={stock_info['j_value']:.3f} < {j_threshold}, turnover={stock_info['turnover_mv5']:.3f} > {turnover_threshold}""")
                else:
                    logger.debug(f"❌ {stock_info['symbol']}: J={stock_info['j_value']:.3f} >= {j_threshold}, turnover={stock_info['turnover_mv5']:.3f} < {turnover_threshold}")
        
        logger.info(f"Stock picking completed. Found {len(selected_stocks)} stocks.")
        return selected_stocks
    
    def save_results(self, selected_stocks: List[Dict], output_file: str = None) -> str:
        """
        Save the selected stocks to a CSV file.
        
        Args:
            selected_stocks (List[Dict]): List of selected stocks
            output_file (str): Output filename (optional)
            
        Returns:
            str: Path to saved file
        """
        if not selected_stocks:
            logger.warning("No stocks to save")
            return ""
        
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"selected_stocks_{timestamp}.csv"
        
        # Create DataFrame
        df_results = pd.DataFrame(selected_stocks)
        
        # Sort by J value (most negative first)
        df_results = df_results.sort_values('j_value')
        
        # Save to CSV
        output_path = os.path.join('data/picked', output_file)
        df_results.to_csv(output_path, index=False)
        logger.info(f"Results saved to: {output_path}")

        symbols = df_results['symbol'].values.tolist()
        # Write to txt file
        with open(f"data/picked/selected_stocks_{timestamp}.txt", 'w') as f:
            for symbol in symbols:
                f.write(symbol + '\n')
        
        return output_path
    
    def print_summary(self, selected_stocks: List[Dict]):
        """Print a summary of the selected stocks."""
        if not selected_stocks:
            print("❌ No stocks found meeting the criteria")
            return
        
        print(f"\n🎯 STOCK SELECTION SUMMARY")
        print("=" * 60)
        print(f"Total stocks selected: {len(selected_stocks)}")
        print(f"Selection criteria: J < 0.0")
        
        # print(f"\n📊 TOP 10 STOCKS (Most Negative J Values):")
        # print("-" * 60)
        
        # Sort by J value and show top 10
        sorted_stocks = sorted(selected_stocks, key=lambda x: x['j_value'])
        
        # for i, stock in enumerate(sorted_stocks[:10]):
        #     print(f"{i+1:2d}. {stock['symbol']:15s} | "
        #           f"J: {stock['j_value']:7.3f} | "
        #           f"K: {stock['k_value']:7.3f} | "
        #           f"D: {stock['d_value']:7.3f} | "
        #           f"Close: {stock['latest_close']:8.3f} | "
        #           f"Date: {stock['latest_date']}")
        
        # if len(selected_stocks) > 10:
        #     print(f"... and {len(selected_stocks) - 10} more stocks")
        
        # Statistics
        # j_values = [stock['j_value'] for stock in selected_stocks]
        # print(f"\n📈 STATISTICS:")
        # print(f"   Average J value: {np.mean(j_values):.3f}")
        # print(f"   Min J value: {np.min(j_values):.3f}")
        # print(f"   Max J value: {np.max(j_values):.3f}")
        # print(f"   Standard deviation: {np.std(j_values):.3f}")

def main():
    """Main function to run the stock picker."""
    
    print("🚀 Stock Picker - KDJ J < 0 Selection")
    print("=" * 50)
    
    try:
        # Initialize stock picker
        picker = StockPicker()
        
        # Pick stocks with J < 0
        selected_stocks = picker.pick_stocks(j_threshold=0.0)
        
        if selected_stocks:
            # Print summary
            picker.print_summary(selected_stocks)
            
            # Save results
            output_file = picker.save_results(selected_stocks)
            
            print(f"\n💾 Results saved to: {output_file}")
            print(f"🎉 Stock picking completed successfully!")
            
        else:
            print("❌ No stocks found meeting the criteria")
            
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    main()
