#!/usr/bin/env python3
"""
NASDAQ Stock Symbol Fetcher

This script fetches stock data from the NASDAQ API and saves it as a JSON file.
API endpoint: https://api.nasdaq.com/api/screener/stocks?tableonly=true&download=true

reference: https://github.com/ranaroussi/yfinance/discussions/1699
"""

import requests
import json
import pandas as pd
from datetime import datetime
import time
import logging
import os
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NASDAQStockFetcher:
    """Class to fetch and process NASDAQ stock data."""
    
    def __init__(self):
        """Initialize the NASDAQ stock fetcher."""
        self.base_url = "https://api.nasdaq.com/api/screener/stocks"
        self.session = requests.Session()
        
        # Set headers to mimic a browser request
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        os.makedirs('data/list_symbol', exist_ok=True)
    
    def fetch_stock_data(self, tableonly: bool = True, download: bool = True) -> Optional[Dict]:
        """
        Fetch stock data from NASDAQ API.
        
        Args:
            tableonly (bool): Whether to return only table data
            download (bool): Whether to treat as download request
        
        Returns:
            Optional[Dict]: JSON response data or None if failed
        """
        try:
            # Prepare query parameters
            params = {
                'tableonly': str(tableonly).lower(),
                'download': str(download).lower()
            }
            
            logger.info(f"Fetching data from NASDAQ API...")
            logger.info(f"URL: {self.base_url}")
            logger.info(f"Parameters: {params}")
            
            # Make the request
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=30
            )
            
            # Check if request was successful
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            logger.info(f"Successfully fetched data from NASDAQ API")
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response size: {len(response.content)} bytes")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def process_stock_data(self, data: Dict) -> pd.DataFrame:
        """
        Process the raw stock data into a structured DataFrame.
        
        Args:
            data (Dict): Raw JSON data from NASDAQ API
        
        Returns:
            pd.DataFrame: Processed stock data
        """
        try:
            logger.info("Processing stock data...")
            
            # Extract the data array from the response
            if 'data' in data and 'rows' in data['data']:
                rows = data['data']['rows']
                logger.info(f"Found {len(rows)} stock entries")
                
                # Convert to DataFrame
                df = pd.DataFrame(rows)
                
                # Clean and standardize column names
                df.columns = df.columns.str.lower().str.replace(' ', '_')
                
                # Add metadata columns
                df['fetched_at'] = datetime.now().isoformat()
                df['source'] = 'NASDAQ API'
                
                logger.info(f"Successfully processed {len(df)} stocks")
                logger.info(f"Columns: {list(df.columns)}")
                
                return df
                
            else:
                logger.error("Unexpected data structure in API response")
                logger.error(f"Available keys: {list(data.keys())}")
                if 'data' in data:
                    logger.error(f"Data keys: {list(data['data'].keys())}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error processing stock data: {e}")
            return pd.DataFrame()
    
    
    def save_as_csv(self, df: pd.DataFrame, filename: str = None) -> str:
        """
        Save the processed stock data as a CSV file.
        
        Args:
            df (pd.DataFrame): Processed stock data
            filename (str): Output filename (optional)
        
        Returns:
            str: Path to saved CSV file
        """
        if filename is None:
            # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            # filename = f"nasdaq_stocks_processed_{timestamp}.csv"
            filename = "list_symbol/us.csv"
        
        filepath = os.path.join('data', filename)
        
        try:
            df.to_csv(filepath, index=False, encoding='utf-8')
            logger.info(f"Processed data saved to: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving CSV file: {e}")
            return ""
    
    def get_stock_summary(self, df: pd.DataFrame) -> Dict:
        """
        Generate a summary of the stock data.
        
        Args:
            df (pd.DataFrame): Stock data DataFrame
        
        Returns:
            Dict: Summary statistics
        """
        try:
            summary = {
                'total_stocks': len(df),
                'columns': list(df.columns),
                'data_types': df.dtypes.to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'sample_data': df.head(5).to_dict('records')
            }
            
            # Add specific stock information if available
            if 'symbol' in df.columns:
                summary['unique_symbols'] = df['symbol'].nunique()
                summary['sample_symbols'] = df['symbol'].head(10).tolist()
            
            if 'market_cap' in df.columns:
                summary['market_cap_stats'] = {
                    'min': df['market_cap'].min() if not df['market_cap'].isna().all() else None,
                    'max': df['market_cap'].max() if not df['market_cap'].isna().all() else None,
                    'mean': df['market_cap'].mean() if not df['market_cap'].isna().all() else None
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return {}
    
    def fetch_and_save(self, save_csv: bool = True) -> Dict:
        """
        Fetch stock data and save it in multiple formats.
        
        Args:
            save_raw (bool): Whether to save raw JSON data
            save_csv (bool): Whether to save processed CSV data
        
        Returns:
            Dict: Summary of the operation
        """
        logger.info("=" * 60)
        logger.info("STARTING NASDAQ STOCK DATA FETCH")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        # Fetch data from API
        raw_data = self.fetch_stock_data()
        
        if raw_data is None:
            logger.error("Failed to fetch data from NASDAQ API")
            return {'success': False, 'error': 'API request failed'}
        
        # Process the data
        df = self.process_stock_data(raw_data)
        
        if df.empty:
            logger.error("Failed to process stock data")
            return {'success': False, 'error': 'Data processing failed'}
        
        # Save data in requested formats
        saved_files = {}
        
        if save_csv:
            csv_file = self.save_as_csv(df)
            if csv_file:
                saved_files['processed_csv'] = csv_file
        
        # Generate summary
        summary = self.get_stock_summary(df)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Final results
        results = {
            'success': True,
            'elapsed_time': elapsed_time,
            'total_stocks': len(df),
            'saved_files': saved_files,
            'summary': summary,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=" * 60)
        logger.info("NASDAQ STOCK DATA FETCH COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Total stocks: {len(df)}")
        logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        logger.info(f"Files saved: {list(saved_files.keys())}")
        
        return results


def main():
    """Main function to fetch and save NASDAQ stock data."""
    
    # Create fetcher instance
    fetcher = NASDAQStockFetcher()
    
    # Fetch and save data
    results = fetcher.fetch_and_save(save_csv=True)
    
    if results['success']:
        print("\n🎉 SUCCESS! NASDAQ stock data has been fetched and saved.")
        print(f"📊 Total stocks: {results['total_stocks']}")
        print(f"⏱️  Time taken: {results['elapsed_time']:.2f} seconds")
        
        print(f"\n📁 Files saved:")
        for file_type, filepath in results['saved_files'].items():
            print(f"  {file_type}: {filepath}")
        
        print(f"\n📈 Data Summary:")
        summary = results['summary']
        print(f"  Columns: {len(summary.get('columns', []))}")
        print(f"  Sample symbols: {summary.get('sample_symbols', [])[:5]}")
        
    else:
        print(f"\n❌ FAILED: {results.get('error', 'Unknown error')}")
    
    return results


if __name__ == "__main__":
    main()