from pandas.core.frame import DataFrame
import pandas as pd
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import time

from constant.indicator import Indicator
from constant.customised_indicator import CustomisedIndicator
from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine
from utils.datetime_util import is_premarket_hours, is_normal_trading_hours, is_postmarket_hours

idx = pd.IndexSlice

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

def fetch_snapshots_from_yfinance(current_date_time, ticker_list):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}
    ticker_to_pct_dict = {}

    start_time = time.time()
    async def retrieve_quotes(session, ticker):
        url = f'https://finance.yahoo.com/quote/{ticker}'
        async with session.get(url, headers=headers) as response:
            assert response.status == 200
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            if is_premarket_hours(current_date_time):
                premarket_pct_change_field = soup.find('fin-streamer', {'data-field': 'preMarketChangePercent'})
                pct_change = premarket_pct_change_field.find('span').string if premarket_pct_change_field else 'NA'
            elif is_normal_trading_hours(current_date_time):
                normal_trading_hour_pct_change_field = soup.findAll('fin-streamer', {'data-field': 'regularMarketChangePercent'})[-1]
                pct_change = normal_trading_hour_pct_change_field.find('span').string if normal_trading_hour_pct_change_field else 'NA'
            elif is_postmarket_hours(current_date_time):
                postmarket_pct_change_field = soup.findAll('fin-streamer', {'data-field': 'postMarketChangePercent'})[-1]
                pct_change = postmarket_pct_change_field.find('span').string if postmarket_pct_change_field else 'NA'
            
            ticker_to_pct_dict[ticker] = pct_change
    
    async def create_session_and_asyn_tasks():
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[retrieve_quotes(session, ticker) for ticker in ticker_list])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_session_and_asyn_tasks())
    logger.debug(f'--- Total snapshots retrieval time from yfinance: {time.time() - start_time} seconds ---')
    logger.debug(f'Snapshots data: {ticker_to_pct_dict}')
    return ticker_to_pct_dict

def append_custom_statistics(historical_data_df: DataFrame) -> DataFrame:
    high_df = historical_data_df.loc[:, idx[:, Indicator.HIGH]]
    low_df = historical_data_df.loc[:, idx[:, Indicator.LOW]]
    close_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
    vol_df = historical_data_df.loc[:, idx[:, Indicator.VOLUME]]
    
    typical_price_df = ((high_df.add(low_df.values)
                                .add(close_df.values))
                                .div(3))
    
    vwap_df = (typical_price_df.mul(vol_df.values)).div(vol_df.cumsum().values).rename(columns={Indicator.HIGH: CustomisedIndicator.VWAP})
    ema_9_df = close_df.ewm(span=9, adjust=False).rename(columns={Indicator.CLOSE: CustomisedIndicator.EMA_9})
    ema_20_df = close_df.ewm(span=20, adjust=False).rename(columns={Indicator.CLOSE: CustomisedIndicator.EMA_20})

    return pd.concat([historical_data_df, 
                        vwap_df, 
                        ema_9_df, 
                        ema_20_df], axis=1)
