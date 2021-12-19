from datetime import datetime
from pandas.core.frame import DataFrame
import pandas as pd
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import time

from pandas.core.indexes.multi import MultiIndex
from pytz import timezone

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from utils.datetime_util import is_normal_trading_hours, is_premarket_hours, is_postmarket_hours
from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine

idx = pd.IndexSlice

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

def fetch_snapshots_from_yfinance(ticker_list):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}
    ticker_to_previous_close_dict = {}

    if len(ticker_list) == 0:
        return {}

    start_time = time.time()
    current_date_time = datetime.now(timezone('US/Eastern')).replace(microsecond=0, tzinfo=None)
    async def retrieve_quotes(session, ticker):
        url = f'https://finance.yahoo.com/quote/{ticker}'
        async with session.get(url, headers=headers) as response:
            assert response.status == 200
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            if is_premarket_hours(current_date_time) or is_postmarket_hours():
                previous_close = soup.findAll('fin-streamer', {'data-field': 'regularMarketPrice'})[-1].string
            elif is_normal_trading_hours(current_date_time):
                previous_close = soup.findAll('td', {'data-test': 'PREV_CLOSE-value'})[-1].string

            if not previous_close:
                previous_close = None

            ticker_to_previous_close_dict[ticker] = previous_close
            logger.debug(f'Yfinance Test, {ticker}, {previous_close}')
    
    async def create_session_and_asyn_tasks():
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[retrieve_quotes(session, ticker) for ticker in ticker_list])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_session_and_asyn_tasks())
    logger.debug(f'--- Total snapshots retrieval time from yfinance: {time.time() - start_time} seconds ---')
    logger.debug(f'Snapshots data dictionary: {ticker_to_previous_close_dict}')
    return ticker_to_previous_close_dict

def append_custom_statistics(historical_data_df: DataFrame, ticker_to_previous_close_dict: dict) -> DataFrame:
    high_df = historical_data_df.loc[:, idx[:, Indicator.HIGH]]
    low_df = historical_data_df.loc[:, idx[:, Indicator.LOW]]
    close_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
    vol_df = historical_data_df.loc[:, idx[:, Indicator.VOLUME]].astype(float, errors = 'raise')
    
    close_pct_df = close_df.pct_change().mul(100).fillna(0).rename(columns={Indicator.CLOSE: CustomisedIndicator.CLOSE_CHANGE})

    typical_price_df = ((high_df.add(low_df.values)
                                .add(close_df.values))
                                .div(3))
    tpv_cumsum_df = typical_price_df.mul(vol_df.values).cumsum()
    vol_cumsum_df = vol_df.cumsum().rename(columns={Indicator.VOLUME: CustomisedIndicator.TOTAL_VOLUME})
    vwap_df = tpv_cumsum_df.div(vol_cumsum_df.values).rename(columns={Indicator.HIGH: CustomisedIndicator.VWAP})

    vol_20_ma_df = vol_df.rolling(window=20).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_20_VOLUME})
    vol_50_ma_df = vol_df.rolling(window=50).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_50_VOLUME})

    ticker_list = close_df.columns.get_level_values(0).unique()
    previous_close_value = [float(ticker_to_previous_close_dict[ticker]) for ticker in ticker_list]
    close_pct_df = ((close_df.sub(previous_close_value)
                            .div(previous_close_value))
                            .mul(100)
                            .round(2)
                            .rename(columns={Indicator.CLOSE: CustomisedIndicator.CLOSE_CHANGE}))
    
    logger.debug('Full close pct DataFrame: \n' + close_pct_df.to_string().replace('\n', '\n\t'))

    return pd.concat([historical_data_df, 
                        close_pct_df,
                        vwap_df, 
                        vol_20_ma_df,
                        vol_50_ma_df,
                        vol_cumsum_df], axis=1)
