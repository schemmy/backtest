import pandas as pd
from yahoo_api import download_stock_data
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_stock_data_threaded(ticker: str, market: str) -> tuple:
    """
    Thread-safe version of download_stock_data for use with ThreadPoolExecutor.
    
    Args:
        ticker (str): Stock ticker symbol
    
    Returns:
        tuple: (ticker, success_status, error_message)
    """
    try:
        logger.info(f"Downloading data for {ticker}...")
        download_stock_data(symbol=ticker, period='1y', output_folder=market)
        logger.info(f"Successfully downloaded {ticker}")
        return (ticker, True, None)
    except Exception as e:
        error_msg = f"Error downloading {ticker}: {str(e)}"
        logger.error(error_msg)
        return (ticker, False, str(e))

def download_stocks_multithreaded(tickers: list, market='a', max_workers: int = 10) -> tuple:
    """
    Download stock data for multiple symbols using multithreading.
    
    Args:
        df (pd.DataFrame): DataFrame containing ticker symbols
        max_workers (int): Maximum number of worker threads
    
    Returns:
        tuple: (successful_symbols, failed_symbols, total_time)
    """
    total_symbols = len(tickers)
    
    logger.info(f"Starting multithreaded download for {total_symbols} symbols with {max_workers} workers")
    start_time = time.time()
    
    successful_symbols = []
    failed_symbols = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_ticker = {
            executor.submit(download_stock_data_threaded, ticker, market): ticker 
            for ticker in tickers
        }
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_ticker):
            ticker, success, error = future.result()
            completed += 1
            
            if success:
                successful_symbols.append(ticker)
                logger.info(f"✓ {ticker} completed ({completed}/{total_symbols})")
            else:
                failed_symbols.append(ticker)
                logger.warning(f"✗ {ticker} failed: {error} ({completed}/{total_symbols})")
            
            # Progress update
            if completed % 10 == 0 or completed == total_symbols:
                success_rate = len(successful_symbols) / completed * 100
                logger.info(f"Progress: {completed}/{total_symbols} ({success_rate:.1f}% success)")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    return successful_symbols, failed_symbols, total_time

def main(market: str = "a"):
    """Main function to download stock data using multithreading."""
    
    # Read the CSV file
    logger.info("Reading symbol list from CSV...")
    df = pd.read_csv("data/list_symbol/{}.csv".format(market))
    if 'ticker_yfinance_format' not in df.columns:
        df['ticker_yfinance_format'] = df['symbol']
    total_symbols = len(df)
    
    logger.info(f"Found {total_symbols} symbols to download")
    
    # Determine optimal number of workers based on system capabilities
    import os
    cpu_count = os.cpu_count()
    max_workers = 1 #min(cpu_count * 1, 20)  # Use 2x CPU cores, but cap at 20
    
    logger.info(f"Using {max_workers} worker threads (CPU cores: {cpu_count})")
    
    # Download stocks using multithreading
    tickers = df['ticker_yfinance_format'].tolist()
     
    successful_symbols, failed_symbols, total_time = download_stocks_multithreaded(tickers, market, max_workers)
    # Print summary
    logger.info("=" * 50)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total symbols: {total_symbols}")
    logger.info(f"Successful: {len(successful_symbols)} ({len(successful_symbols)/total_symbols*100:.1f}%)")    
    logger.info(f"Failed: {len(failed_symbols)} ({len(failed_symbols)/total_symbols*100:.1f}%)")
    logger.info(f"Total time: {total_time:.2f} seconds")
    logger.info(f"Average time per symbol: {total_time/total_symbols:.3f} seconds")
    logger.info(f"Speedup vs single-threaded: ~{max_workers}x (theoretical)")
    
    successful_symbols, failed_symbols, _ = download_stocks_multithreaded(failed_symbols, market, max_workers)
    # Print summary
    logger.info("=" * 50)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total symbols: {total_symbols}")
    logger.info(f"Successful: {len(successful_symbols)} ({len(successful_symbols)/total_symbols*100:.1f}%)")    
    logger.info(f"Failed: {len(failed_symbols)} ({len(failed_symbols)/total_symbols*100:.1f}%)")
    logger.info(f"Total time: {total_time:.2f} seconds")
    logger.info(f"Average time per symbol: {total_time/total_symbols:.3f} seconds")
    logger.info(f"Speedup vs single-threaded: ~{max_workers}x (theoretical)")
    
    # if failed_symbols:
        # logger.info(f"\nFailed symbols: {failed_symbols}")
    
    logger.info(f"\nResults saved to:")
    logger.info(f"  - data/list_symbol/a_success.csv ({len(successful_symbols)} symbols)")
    if failed_symbols:
        logger.info(f"  - data/list_symbol/a_failed.csv ({len(failed_symbols)} symbols)")

if __name__ == "__main__":
    main(market="us")