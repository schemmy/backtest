import yfinance as yf
import pandas as pd

def download_stock_data(symbol: str, period: str = '1y', output_folder: str = None) -> pd.DataFrame:
    """
    Download stock data from Yahoo Finance and optionally save to CSV.
    
    Args:
        symbol (str): Stock symbol (e.g., 'MSFT')
        period (str): Time period to download (default: '5y')
        output_file (str, optional): If provided, save data to this CSV file
    
    Returns:
        pd.DataFrame: DataFrame containing the stock data
    """
    df = yf.download([symbol], period=period).reset_index()
    # print(df)
    # Rename columns to lowercase
    df.columns = ['date', 'close', 'high', 'low', 'open', 'volume']
    
    # Round all float columns to 3 decimal places
    float_columns = ['close', 'high', 'low', 'open']
    df[float_columns] = df[float_columns].round(3)
    
    df.to_csv('data/stock_data/{}/{}.csv'.format(output_folder, symbol.lower()), sep=',', index=False)
    
    return df

if __name__ == "__main__":
    # df = download_stock_data("600247.SH", "5y")
    df = download_stock_data("PRGS", "1y", output_folder='test')
    print(df.head())