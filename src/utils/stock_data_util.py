from pandas.core.frame import DataFrame
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import time

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from utils.datetime_util import is_normal_trading_hours, is_premarket_hours, is_postmarket_hours
from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine

idx = pd.IndexSlice

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

def update_snapshots(current_datetime, 
                    ticker_to_snapshots_dict, 
                    scanner_result_list) -> None:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}

    async def retrieve_quotes(session, ticker):
        url = f'https://finance.yahoo.com/quote/{ticker}'
        async with session.get(url, headers=headers) as response:
            assert response.status == 200
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            if is_premarket_hours(current_datetime) or is_postmarket_hours(current_datetime):
                previous_close = soup.findAll('fin-streamer', {'data-field': 'regularMarketPrice'})[-1].string
            elif is_normal_trading_hours(current_datetime):
                previous_close = soup.findAll('td', {'data-test': 'PREV_CLOSE-value'})[-1].string
            else:
                previous_close = soup.findAll('fin-streamer', {'data-field': 'regularMarketPrice'})[-1].string

            result_dict[ticker] = previous_close.replace(',', '')
    
    async def create_session_and_asyn_tasks(ticker_list):
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[retrieve_quotes(session, ticker) for ticker in ticker_list])

    retrive_ticker_list = list(set(scanner_result_list) - set([ticker for ticker in ticker_to_snapshots_dict]))
    
    if len(retrive_ticker_list) > 0:
        loop = asyncio.get_event_loop()
        start_time = time.time()
        result_dict = {}
        logger.debug(f'Old ticker to previous close dict: {ticker_to_snapshots_dict}')
        logger.debug(f'Retrieve previous close for tickers: {retrive_ticker_list}')
        loop.run_until_complete(create_session_and_asyn_tasks(retrive_ticker_list))
        ticker_to_snapshots_dict.update(result_dict)
        logger.debug(f'--- Total snapshots retrieval time from yfinance: {time.time() - start_time} seconds ---')
        logger.debug(f'Retrieved snapshots data dictionary: {result_dict}')
        logger.debug(f'Updated ticker to previous close dict: {ticker_to_snapshots_dict}')

def append_custom_statistics(candle_df: DataFrame, ticker_to_snapshots_dict: dict) -> DataFrame:
    open_df = candle_df.loc[:, idx[:, Indicator.OPEN]].rename(columns={Indicator.OPEN: RuntimeIndicator.COMPARE})
    high_df = candle_df.loc[:, idx[:, Indicator.HIGH]]
    low_df = candle_df.loc[:, idx[:, Indicator.LOW]]
    close_df = candle_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
    vol_df = candle_df.loc[:, idx[:, Indicator.VOLUME]].astype(float, errors = 'raise')
    
    typical_price_df = ((high_df.add(low_df.values)
                                .add(close_df.values))
                                .div(3))
    tpv_cumsum_df = typical_price_df.mul(vol_df.values).cumsum()
    vol_cumsum_df = vol_df.cumsum().rename(columns={Indicator.VOLUME: CustomisedIndicator.TOTAL_VOLUME})
    vwap_df = tpv_cumsum_df.div(vol_cumsum_df.values).rename(columns={Indicator.HIGH: CustomisedIndicator.VWAP})

    vol_20_ma_df = vol_df.rolling(window=20, min_periods=1).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_20_VOLUME})
    vol_50_ma_df = vol_df.rolling(window=50, min_periods=1).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_50_VOLUME})

    ticker_list = close_df.columns.get_level_values(0)
    previous_close_value = [float(ticker_to_snapshots_dict[ticker]) for ticker in ticker_list]
    previous_close_pct_df = (((close_df.sub(previous_close_value))
                                    .div(previous_close_value))
                                    .mul(100)).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.PREVIOUS_CLOSE_CHANGE})

    close_pct_df = close_df.pct_change().mul(100).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CLOSE_CHANGE})
    close_pct_df.iloc[[0]] = previous_close_pct_df.iloc[[0]]

    green_candle_df = (close_df > open_df).replace({True: CandleColour.GREEN, False: np.nan})
    red_candle_df = (close_df < open_df).replace({True: CandleColour.RED, False: np.nan})
    flat_candle_df = (close_df == open_df).replace({True: CandleColour.GREY, False: np.nan})
    colour_df = ((green_candle_df.fillna(red_candle_df))
                                    .fillna(flat_candle_df)
                                    .rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CANDLE_COLOUR}))

    close_above_open_boolean_df = (close_df > open_df)
    high_low_diff_df = high_df.sub(low_df.values)
    close_above_open_upper_body_df = close_df.where(close_above_open_boolean_df.values)
    open_above_close_upper_body_df = open_df.where((~close_above_open_boolean_df).values)
    upper_body_df = close_above_open_upper_body_df.fillna(open_above_close_upper_body_df)
    
    close_above_open_lower_body_df = open_df.where(close_above_open_boolean_df.values)
    open_above_close_lower_body_df = close_df.where((~close_above_open_boolean_df).values)
    lower_body_df = close_above_open_lower_body_df.fillna(open_above_close_lower_body_df)

    body_diff_df = upper_body_df.sub(lower_body_df.values)
    marubozu_ratio_df = (body_diff_df.div(high_low_diff_df.values)).mul(100).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.MARUBOZU_RATIO})
    
    return pd.concat([candle_df, 
                        close_pct_df,
                        previous_close_pct_df,
                        colour_df,
                        marubozu_ratio_df,
                        vwap_df, 
                        vol_20_ma_df,
                        vol_50_ma_df,
                        vol_cumsum_df], axis=1)