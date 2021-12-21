from datetime import datetime

from pytz import timezone
from constant.timeframe import Timeframe
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import ContractDetails
from ibapi.wrapper import EWrapper

import pandas as pd
import time

from constant.indicator.indicator import Indicator
from constant.filter.pattern import Pattern

from factory.pattern_analyser_factory import PatternAnalyserFactory

from utils.datetime_util import convert_datetime_format_str, get_trading_session_start_datetime
from utils.filter_util import get_contract
from utils.log_util import get_logger
from utils.stock_data_util import append_custom_statistics, update_snapshots

logger = get_logger(console_log=False)

class IBConnector(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.__timeframe_list = [Timeframe.ONE_MINUTE, Timeframe.FIVE_MINUTE]
        self.__scanner_result_list = []
        self.__ticker_to_snapshots = {}
        self.__start_time = None

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)

        ''' Callbacks to EWrapper with errorId as -1 do not represent true 'errors' but only 
        notification that a connector has been made successfully to the IB market data farms. '''
        if errorCode == -1:
            connect_success_msg = f'reqId: {reqId} Connect Success, {errorString}'
            logger.debug(connect_success_msg)
        elif errorCode == 1100:
            error_msg = 'TWS API Connection Lost'
            logger.exception(error_msg)
            raise Exception(error_msg)

    def historicalData(self, reqId, bar):
        open = bar.open
        high = bar.high
        low = bar.low
        close = bar.close
        volume = bar.volume * 100
        dt = bar.date
        timeframe_idx = (reqId - 1) // len(self.__scanner_result_list)
        
        #rank = reqId - (timeframe_idx * len(self.__scanner_result_list)) - 1
        #logger.debug(f'Action: historicalData, reqId: {reqId}, Timeframe index: {timeframe_idx}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}, Bar datetime: {convert_datetime_format_str(dt)}, Open: {open}, High: {high}, Low: {low}, Close: {close}, Volume: {volume}')
        self.__timeframe_idx_to_ohlcv_list_dict[timeframe_idx].append([open, high, low, close, volume])
        self.__timeframe_idx_to_datetime_list_dict[timeframe_idx].append(dt)

    #Marks the ending of historical bars reception.
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        timeframe_idx = (reqId - 1) // len(self.__scanner_result_list)
        rank = reqId - (timeframe_idx * len(self.__scanner_result_list)) - 1

        ohlcv_list = self.__timeframe_idx_to_ohlcv_list_dict[timeframe_idx]
        datetime_list = self.__timeframe_idx_to_datetime_list_dict[timeframe_idx]
        datetime_index = pd.DatetimeIndex(datetime_list)
        ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__scanner_result_list[rank]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.VOLUME]])
        candle_df = pd.DataFrame(ohlcv_list, columns=ticker_to_indicator_column, index=datetime_index)
        self.__timeframe_idx_to_concat_df_list_dict[timeframe_idx].append(candle_df)

        self.__timeframe_idx_to_ohlcv_list_dict[timeframe_idx] = []
        self.__timeframe_idx_to_datetime_list_dict[timeframe_idx] = []
        logger.debug(f'Action: historicalDataEnd, reqId: {reqId}, Timeframe index: {timeframe_idx}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}')

        is_all_candle_retrieved = all([len(concat_df_list) == len(self.__scanner_result_list) for concat_df_list in self.__timeframe_idx_to_concat_df_list_dict.values()])
        
        if is_all_candle_retrieved:
            for concat_df_list in self.__timeframe_idx_to_concat_df_list_dict.values():
                complete_historical_data_df = append_custom_statistics(pd.concat(concat_df_list, axis=1), self.__ticker_to_snapshots)
                logger.debug('Full candle DataFrame: \n' + complete_historical_data_df.to_string().replace('\n', '\n\t'))
                pattern_list = [Pattern.UNUSUAL_RAMP_UP]
                for pattern in pattern_list:
                    pattern_analyzer = PatternAnalyserFactory.get_pattern_analyser(pattern, complete_historical_data_df)
                    pattern_analyzer.analyse()
            
            logger.debug(f'--- Total historical data retrieval and analysis time: {time.time() - self.__start_time} seconds ---')

    def __get_historical_data_and_analyse(self):
        current_datetime = datetime.now(timezone('US/Eastern')).replace(microsecond=0, tzinfo=None)
        update_snapshots(current_datetime, self.__ticker_to_snapshots, self.__scanner_result_list)
        
        req_id_multiplier = len(self.__scanner_result_list)
        retrieve_candle_start_datetime = get_trading_session_start_datetime()
        timeframe_interval = (current_datetime - retrieve_candle_start_datetime).seconds
        
        self.__timeframe_idx_to_ohlcv_list_dict = {}
        self.__timeframe_idx_to_datetime_list_dict = {}
        self.__timeframe_idx_to_concat_df_list_dict = {}
        
        for timeframe_idx, timeframe in enumerate(self.__timeframe_list):
            self.__timeframe_idx_to_ohlcv_list_dict[timeframe_idx] = []
            self.__timeframe_idx_to_datetime_list_dict[timeframe_idx] = []
            self.__timeframe_idx_to_concat_df_list_dict[timeframe_idx] = []

            for index, ticker in enumerate(self.__scanner_result_list, start=1):
                candle_req_id = (timeframe_idx * req_id_multiplier) + index
                contract = get_contract(ticker)
                self.reqHistoricalData(candle_req_id, contract, '', timeframe_interval, timeframe.value, 'TRADES', 0, 1, False, [])
    
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        if rank == 0:
            if self.__start_time != None:
                logger.debug(f'Scanner refresh interval time: {time.time() - self.__start_time} seconds')
            self.__start_time = time.time()
            self.__scanner_result_list = []
        
        self.__scanner_result_list.append(contractDetails.contract.symbol)

    #scannerDataEnd marker will indicate when all results have been delivered.
    #The returned results to scannerData simply consists of a list of contracts, no market data field (bid, ask, last, volume, ...).
    def scannerDataEnd(self, reqId: int):
        logger.debug(f'Action: scannerDataEnd, reqId: {reqId}, Result length: {len(self.__scanner_result_list)}, Result: {self.__scanner_result_list}')
        self.__get_historical_data_and_analyse()