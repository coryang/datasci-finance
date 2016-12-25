#imports
import pandas as pd
import pandas_datareader.data as web
import datetime

#configs
cfg_data_start = datetime.datetime(2016, 1, 4)

cfg_data_end = datetime.datetime(2016, 1, 8)

cfg_tickers = ["GS","MS","GOOG","FB"]

#data load
df_px_raw = web.DataReader(cfg_tickers, 'yahoo', cfg_data_start, cfg_data_end)

df_px = df_px_raw.to_frame()

df_px = df_px.reset_index().rename(columns={'minor':'Ticker'})

df_rtn = df_px.pivot('Date','Ticker','Close').pct_change()

