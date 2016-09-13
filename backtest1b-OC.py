import pandas as pd
import numpy as np
import sqlalchemy
import datetime
import matplotlib.pyplot as plt

import tia.bbg.datamgr as dm
mgr = dm.BbgDataManager()

#********************************************
'''
todo:
entry/exit pnl. day exit: doesn't take close-open pnl. day exit: does it exit on close?
'''
#********************************************

#config
cfg_date_start = datetime.date(2013,1,1)
cfg_date_end = datetime.date(2013,6,1)
cfg_ticker_max = None
cfg_ticker_OC = True
cfg_trade_size = 1000.0
cfg_trade_cost = 15.0/100.0/100.0

'''
#load MS data
df_DB_raw = pd.read_csv('DB US_LASR.csv', parse_dates=['POINTDATE'])

#postprocess MS data
df_DB = df_DB_raw.copy(True)
df_DB['ticker'] = df_DB['SEDOL']+' US Equity'
df_DB['date'] = df_DB['POINTDATE']
df_DB['date_str'] = df_DB['date'].dt.strftime('%Y-%m-%d')

if(cfg_ticker_max is not None):
    df_DB = df_DB.ix[df_DB['ticker'].isin(df_DB['ticker'].unique()[0:cfg_ticker_max]),:]
else:
    df_DB = df_DB
elif cfg_ticker_OC:
    import sqlalchemy

    sql_engine_mysql_out = sqlalchemy.create_engine(
        'postgresql://postgres:Rd510m0x00E63106DLSq@14.17.244.181/mercury')
    sql_cmd = "select distinct(symbol) from data_in_positions where investment_type='EQUITY';"
    df_tickers_oca = pd.read_sql_query(sql_cmd, sql_engine_mysql_out)

    cfg_bbg_tickers = df_DB['ticker'].unique()
    sids = mgr[cfg_bbg_tickers]
    df_bbg_ocasym = sids.get_attributes(['TICKER'], ignore_security_error=1)
    df_bbg_ocasym.columns = ['ocasym']

    df_portfolio = df_portfolio.ix[df_portfolio['ocasym'].isin(df_tickers_oca['symbol'].unique()), :]
    print df_portfolio.shape


df_DB['signal'] = df_DB.groupby(['date'])['SCORE'].transform(lambda x: pd.cut(x.rank() / len(x),3,labels=range(0,3)))

df_DB.to_pickle('df_DB.pkl')
'''

df_DB = pd.read_pickle('df_DB.pkl')


#load bbg data
'''
cfg_date_start = df_DB['date'].min()
cfg_date_end = df_DB['date'].max()

cfg_bbg_tickers = df_DB['ticker'].unique()
sids = mgr[cfg_bbg_tickers]
df_bbg_ocasym = sids.get_attributes(['TICKER'], ignore_security_error=1)
df_bbg_ocasym.columns = ['ocasym']

#sids.CUR_MKT_CAP

df_bbg_px_raw = sids.get_historical(['PX_OPEN','PX_LAST','EQY_WEIGHTED_AVG_PX'], cfg_date_start, cfg_date_end, ignore_security_error=1, non_trading_day_fill_option="ALL_CALENDAR_DAYS", non_trading_day_fill_method="PREVIOUS_VALUE")
print '*finished download'

df_bbg_px_raw.to_pickle('df_bbg_px_raw.pkl')
df_bbg_px_raw = pd.read_pickle('df_bbg_px_raw.pkl')

print df_bbg_px_raw.head(50)

#bbg postprocess
df_bbg_px = df_bbg_px_raw.unstack(level=0).reset_index()
df_bbg_px.columns = ('ticker','field','date','value')
df_bbg_px = df_bbg_px.pivot_table(index=['date','ticker'],values='value',columns='field').reset_index()
df_bbg_px['PX']=df_bbg_px['EQY_WEIGHTED_AVG_PX']
df_bbg_px = df_bbg_px.dropna(subset=['PX_LAST','PX_OPEN'])

df_bbg_px = df_bbg_px.merge(df_bbg_ocasym,left_on='ticker',right_index=True)

#save bbg
df_bbg_px.to_pickle('df_bbg_px.pkl')
#print df_bbg_px.head(50)
#df_bbg_px.ix[df_bbg_px['ticker'].isin(df_bbg_px['ticker'][0:100]),:].to_pickle('df_bbg_px_100.pkl')
'''
#load bbg data
df_bbg_px = pd.read_pickle('df_bbg_px.pkl')

#merge bbg with MS data
df_portfolio = df_bbg_px.merge(df_DB, how='left', left_on=['date','ticker'], right_on=['date','ticker'])
df_portfolio = df_portfolio.sort_values(['date','ticker'])

# filter only OC positions
# TODO: don't load data for this


def apply_trade_signals1(df):
    df['SCORE']=df['SCORE'].ffill()
    df['signal']=df['signal'].ffill()
    return df

def apply_trade_signals2(df):
    df['pos'] = 0.0
    df['shares'] = 0.0
    #df.ix[df['most'] == 5, 'pos'] = -1.0
    #df.ix[df['most'] == 1, 'pos'] = 1.0
    df.ix[df['signal'] == 0, 'pos'] = -1.0
    df.ix[df['signal'] == 2, 'pos'] = 1.0
    return df

def apply_trade_signals3(df):
    #prepare signals
    df['pos'] = df['pos'].shift()
    df['pos_abs'] = df['pos'].abs()
    df['pos'].values[0] = 0.0 #manually set first value
    df['pos_chg'] = (df['pos']!=df['pos'].shift()).astype(int)
    df['pos_chg'].values[0] = 0.0 #manually set first value

    #position sizing
    df['shares'] = cfg_trade_size / df['PX_OPEN'] * df['pos'] * df['pos_chg']
    #todo: rebal every month

    #use same number of shares for every trade
    df.ix[df['pos_chg']==0,'shares']=np.nan
    df['pos_chg_agg'] = df['pos_chg'].cumsum()
    df['shares'] = df.groupby('pos_chg_agg')['shares'].transform(lambda x: x.ffill())

    #calculate pnl
    df['pnl_gross'] = df['shares']*(df['PX_LAST']-df['PX_LAST'].shift())
    df['pnl_commission'] = -df['pos_chg']*cfg_trade_cost*cfg_trade_size
    df['pnl'] = df['pnl_gross'] + df['pnl_commission']
    return df

def apply_trade_signals4(df):
    df['net'] = df['PX_LAST']*df['shares']
    df['gross'] = df['PX_LAST']*df['shares'].abs()
    return df

df_trade = df_portfolio.groupby('ticker').apply(apply_trade_signals1)
df_trade = apply_trade_signals2(df_trade)
df_trade = df_trade.groupby('ticker').apply(apply_trade_signals3)
df_trade = apply_trade_signals4(df_trade)
print df_trade.head(10)
print df_trade.tail(10)
#df_trade.to_csv('test.csv')
#print df_trade.sort_values(['ticker','date']).head(100)
#df_trade.ix[df_trade['ticker'].isin(df_trade['ticker'].unique()[0:2])].sort_values(['ticker','date']).to_csv('test.csv')
#df_trade.ix[df_trade['date'].isin(df_trade['date'].unique()[0:2])].to_csv('test.csv')

plt.figure()
df_trade.groupby('date')['pnl'].sum().cumsum().plot()
df_trade.groupby('date')['pnl'].sum().plot()
plt.figure()
df_trade.groupby('date')['net'].sum().plot()
df_trade.groupby('date')['gross'].sum().plot()
plt.figure()
df_trade.groupby('date')['pos'].sum().plot()
df_trade.groupby('date')['pos_abs'].sum().plot()

print df_trade.groupby('date')['pos'].sum().mean()
print df_trade.groupby('date')['pos_abs'].sum().mean()

df_return = pd.DataFrame({'pnl_agg':df_trade.groupby('date')['pnl'].sum().cumsum(),'gross':df_trade.groupby('date')['gross'].sum()})
df_return['capital']=df_return['gross']/2.0
df_return['rtn_agg']=df_return['pnl_agg']/df_return['capital']
df_return['rtn_dd']=(1.0+df_return['rtn_agg'])/(1.0+df_return['rtn_agg']).cummax()-1

plt.figure()
df_return['rtn_agg'].plot()
df_return['rtn_dd'].plot()

