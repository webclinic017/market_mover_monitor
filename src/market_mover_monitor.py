from constant.indicator import Indicator
from ibapi import contract
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.scanner import ScannerSubscription
from ibapi.wrapper import EWrapper

import pandas as pd
from pytz import timezone
import time
import asyncio

from utils.date_util import get_trading_interval
from utils.file_util import clean_txt_file_content
from utils.log_util import get_logger

logger = get_logger()

class IBConnector(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.__scanner_ticker_list = []
        self.__datetime_list = []
        self.__ohlcv_data_list = []
        self.__concat_df_list = []
        self.__full_candle_df = None
        self.__start_time = time.time()

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)

        ''' Callbacks to EWrapper with errorId as -1 do not represent true 'errors' but only 
        notification that a connector has been made successfully to the IB market data farms. '''
        if errorCode == -1:
            connect_success_msg = f'reqId: {reqId} Connect Success, {errorString}'
            logger.debug(connect_success_msg)
        else:
            error_msg = f'reqId: {reqId}, Error Cause: {errorString}'
            logger.exception(error_msg)

    def __clean_temp_candle_data(self):
        self.__datetime_list = []
        self.__ohlcv_data_list = []
 
    def historicalData(self, reqId, bar):
        open = bar.open
        high = bar.high
        low = bar.low
        close = bar.close
        volume = bar.volume * 100
        dt = bar.date

        self.__ohlcv_data_list.append([open, high, low, close, volume])

        if dt not in self.__datetime_list:
            self.__datetime_list.append(dt)

    #Marks the ending of historical bars reception,
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        #logger.debug(f'Request: historicalData completed, reqId: {reqId}, Ticker: {self.__scanner_ticker_list[reqId - 1]}, Bar datatime: {self.__datetime_list[reqId - 1]}')
        ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__scanner_ticker_list[reqId - 1]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.VOLUME]])
        datetime_index = pd.DatetimeIndex(self.__datetime_list)
        
        individual_ticker_candle_df = pd.DataFrame(self.__ohlcv_data_list, columns=ticker_to_indicator_column, index=datetime_index)
        self.__concat_df_list.append(individual_ticker_candle_df)
        self.__clean_temp_candle_data()
        
        if len(self.__concat_df_list) == len(self.__scanner_ticker_list):
            self.__construct_full_candle_df()
    
    def ___req_histoical_data_by_ticker_list(self):
        for index, ticker in enumerate(self.__scanner_ticker_list):
            contract = Contract()
            contract.symbol = ticker
            contract.secType = 'STK'
            contract.exchange = 'SMART'
            contract.currency = 'USD'
            self.reqHistoricalData(index + 1, contract, '', '1 D', "1 day", "TRADES", 0, 1, False, [])

            #interval = get_trading_interval(timezone('US/Eastern'))
            #interval_str = str(interval) + ' S'
            #self.reqHistoricalData(index + 1, contract, '', interval_str, "1 min", "TRADES", 0, 1, False, [])
    
    def __construct_full_candle_df(self):
        self.__full_candle_df = pd.concat(self.__concat_df_list, axis=1)
        self.__concat_df_list = []
        print('sleeping...')
        time.sleep(60)
        print('sleep done')

    #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
    #The returned results to scannerData simply consists of a list of contracts, no market data field (bid, ask, last, volume, ...)
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        if rank == 0:
            print('start scan')
            if self.__start_time is not None:
                print(f'scan data time interval: {time.time() - self.__start_time}')
                self.__start_time = time.time()
        self.__scanner_ticker_list.append(contractDetails.contract.symbol)
    
    def scannerDataEnd(self, reqId: int):
        logger.debug(f'Request: scannerData completed, reqId: {reqId}, Ticker list length: {len(self.__scanner_ticker_list)}, Result: {self.__scanner_ticker_list}')
        self.___req_histoical_data_by_ticker_list()
        print('all request done')
        #logger.debug(f'Concat DataFrame list length: {len(self.__concat_df_list)}')

def main():
    log_dir = 'log.txt'
    clean_txt_file_content(log_dir)
    connector = IBConnector()
    connector.connect('127.0.0.1', 7496, 0)

    scannerFilter = ScannerSubscription()
    #Top Gainers
    scannerFilter.scanCode = 'TOP_PERC_GAIN'
    #US Stocks
    scannerFilter.instrument = 'STK'
    #Exclude OTC Stocks
    scannerFilter.locationCode = 'STK.US.MAJOR' 
    scannerFilter.abovePrice = 0.3
    scannerFilter.aboveVolume = 10000
    scannerFilter.numberOfRows = 20
    
    connector.reqScannerSubscription(0, scannerFilter, [], [])
    connector.run()

main()