import asyncio
import aiohttp
import time
from pandas.core.frame import DataFrame

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

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

