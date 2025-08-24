# https://github.com/1nchaos/adata
import adata
import pandas as pd

res_df = adata.stock.info.all_code()
# res_df = pd.read_csv("data/list_symbol/a.csv")
res_df['stock_code'] = res_df['stock_code'].astype(str)
res_df['exchange'] = res_df['exchange'].replace('SH', 'SS')
res_df['ticker_yfinance_format'] = res_df['stock_code'] + '.' + res_df['exchange']

res_df.to_csv("data/list_symbol/a.csv", index=False)
