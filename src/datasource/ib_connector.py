from pandas.core.frame import DataFrame
from pytz import timezone
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import ContractDetails
from ibapi.wrapper import EWrapper

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
import requests
import time
import os

from constant.indicator import Indicator
from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine
from utils.datetime_util import convert_datetime_format_str, get_trading_interval, is_normal_trading_hours, is_premarket_hours
from utils.filter_util import get_contract

idx = pd.IndexSlice

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()
session = requests.Session()
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'
}

class IBConnector(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.__scanner_result_list = []

        self.__ohlcav_list = []
        self.__timeframe_list = []
        self.__concat_candle_df_list = []

        #self.__preivious_close_concat_list = []

        #self.__end_time = None

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)

        ''' Callbacks to EWrapper with errorId as -1 do not represent true 'errors' but only 
        notification that a connector has been made successfully to the IB market data farms. '''
        if errorCode == -1:
            connect_success_msg = f'reqId: {reqId} Connect Success, {errorString}'
            logger.debug(connect_success_msg)
        else:
            error_msg = f'Request Error, reqId: {reqId}, Error Cause: {errorString}'
            logger.exception(error_msg)
 
    def historicalData(self, reqId, bar):
        rank = reqId - 1  
        open = bar.open
        high = bar.high
        low = bar.low
        close = bar.close
        avg = bar.wap
        volume = bar.volume * 100
        dt = bar.date

        self.__ohlcav_list.append([open, high, low, close, avg, volume])
        self.__timeframe_list.append(dt)
        #logger.debug(f'Action: historicalData, reqId: {reqId}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}, Bar datetime: {convert_datetime_format_str(dt)}, Open: {open}, High: {high}, Low: {low}, Close: {close}, Avgerage: {avg}, Volume: {volume}')

    #Marks the ending of historical bars reception.
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        rank = reqId - 1
        ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__scanner_result_list[rank]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.BAR_AVERAGE, Indicator.VOLUME]])
        datetime_index = pd.DatetimeIndex(self.__timeframe_list)
        candle_df = pd.DataFrame(self.__ohlcav_list, columns=ticker_to_indicator_column, index=datetime_index)
        self.__concat_candle_df_list.append(candle_df)

        self.__ohlcav_list = []
        self.__timeframe_list = []

        if len(self.__concat_candle_df_list) == len(self.__scanner_result_list):
            one_minute_historical_data_df = pd.concat(self.__concat_candle_df_list, axis=1)
            logger.debug('Full one minute historical DataFrame: \n' + one_minute_historical_data_df.to_string().replace('\n', '\n\t'))

            for ticker in self.__scanner_result_list:
                feed_response = session.get(f'https://finance.yahoo.com/quote/{ticker}', headers=headers)
                feed_contents = feed_response.text
                soup = BeautifulSoup(feed_contents, 'html.parser')

                current_date_time = datetime.now(timezone('US/Eastern')).replace(microsecond=0, tzinfo=None)
                
                if is_premarket_hours(current_date_time):
                    pct_change = soup.find('fin-streamer', {'data-field': 'preMarketChangePercent'}).find('span').string
                elif is_normal_trading_hours(current_date_time):
                    pct_change = soup.findAll('fin-streamer', {'data-field': 'regularMarketChangePercent'})[-1].find('span').string

                logger.debug(f'Action: YFinance request, Ticker: {ticker}, Percent change: {pct_change}')

    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        if rank == 0:
            self.__start_time = time.time()
            self.__scanner_result_list = []
            self.__concat_candle_df_list = []

        self.__scanner_result_list.append(contractDetails.contract.symbol)

    #scannerDataEnd marker will indicate when all results have been delivered.
    #The returned results to scannerData simply consists of a list of contracts, no market data field (bid, ask, last, volume, ...).
    def scannerDataEnd(self, reqId: int):
        logger.debug(f'Action: scannerDataEnd, reqId: {reqId}, Ticker list length: {len(self.__scanner_result_list)}, Result: {self.__scanner_result_list}')

        self.__get_historical_candle_data_df()
        #self.__get_previous_close()
    
    def __get_historical_candle_data_df(self):
        interval = get_trading_interval()
        interval_str = str(interval) + ' S'

        for index, ticker in enumerate(self.__scanner_result_list, start=1):
            contract = get_contract(ticker)
            self.reqHistoricalData(index, contract, '', interval_str, '1 min', 'TRADES', 0, 1, False, [])
    
    '''
    def __compute_complete_historical_data(historical_candle_data_df: DataFrame, previous_close_df: DataFrame):
        print()
    
    def __analyse(self):
        new_ticker_list = list(set(self.__scanner_result_list) - set(self.__previous_scanner_result_list)) if len(self.__previous_scanner_result_list) > 0 else []
        self.__previous_scanner_result_list = self.__scanner_result_list
        os.system('cls')
    '''
    
    '''
    def __get_previous_close(self):
        for index, ticker in enumerate(self.__scanner_result_list, start=20):
            contract = get_contract(ticker)
            self.reqMktData(index, contract, '', False, False, [])

    #Market data tick price callback. Handles all price related ticks.
    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 9:
            rank = reqId - len(self.__scanner_result_list)
            logger.debug(f'Action: tickerPrice, reqId: {reqId}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}, Previous close price: {price}')

            #tickSize are reported as -1, this indicates that there is no data currently available.
            value = price if price != -1 else np.nan
            self.__preivious_close_concat_list.append(value)

        if len(self.__preivious_close_concat_list) == len(self.__scanner_result_list):
            ticker_to_indicator_column = pd.MultiIndex.from_product([self.__scanner_result_list, [Indicator.CLOSE]])
            date_index = pd.DatetimeIndex([datetime.strftime((datetime.now() - timedelta(1)), '%Y-%m-%d')])
            previous_close_df = pd.DataFrame([self.__preivious_close_concat_list], columns=ticker_to_indicator_column, index=date_index)
            
            logger.debug('Full previous close DataFrame: \n' + previous_close_df.to_string().replace('\n', '\n\t'))
            logger.debug(f'--- Total data computaton and anaylsis time: {time.time() - self.__start_time} seconds ---')
    '''

    '''
    def fundamentalData(self, reqId: TickerId, data: str):
        print(data)
    
        self.reqFundamentalData(self, reqId:TickerId , contract:Contract,
                                reportType:str, fundamentalDataOptions:TagValueList)
    '''