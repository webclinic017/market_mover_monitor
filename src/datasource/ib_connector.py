from constant.timeframe import Timeframe
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import ContractDetails
from ibapi.wrapper import EWrapper

from constant.indicator import Indicator

import pandas as pd

from utils.datetime_util import convert_datetime_format_str, get_trading_interval
from utils.filter_util import get_contract
from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine
from utils.stock_data_util import fetch_snapshots_from_yfinance

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

class IBConnector(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.__scanner_result_list = []
        self.__previous_scanner_result_list = []

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
        open = bar.open
        high = bar.high
        low = bar.low
        close = bar.close
        avg = bar.wap
        volume = bar.volume * 100
        dt = bar.date
        timeframe_idx = (reqId - 1) // len(self.__scanner_result_list)
        
        self.__timeframe_idx_to_ohlvav_list_dict[timeframe_idx].append([open, high, low, close, avg, volume])
        self.__timeframe_idx_to_datetime_list_dict[timeframe_idx].append(dt)

    #Marks the ending of historical bars reception.
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        timeframe_idx = (reqId - 1) // len(self.__scanner_result_list)
        rank = reqId - (timeframe_idx * len(self.__scanner_result_list)) - 1

        ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__scanner_result_list[rank]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.BAR_AVERAGE, Indicator.VOLUME]])
        ohlcav_list = self.__timeframe_idx_to_ohlvav_list_dict[timeframe_idx]
        datetime_list = self.__timeframe_idx_to_datetime_list_dict[timeframe_idx]
        datetime_index = pd.DatetimeIndex(datetime_list)
        candle_df = pd.DataFrame(ohlcav_list, columns=ticker_to_indicator_column, index=datetime_index)

        logger.debug(f'Action: historicalDataEnd, reqId: {reqId}, Timeframe index: {timeframe_idx}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}')

        self.__timeframe_idx_to_concat_df_list_dict[timeframe_idx].append(candle_df)
        self.__timeframe_idx_to_ohlvav_list_dict[timeframe_idx] = []
        self.__timeframe_idx_to_datetime_list_dict[timeframe_idx] = []

        is_all_candle_retrieved = all([len(concat_df_list) == len(self.__scanner_result_list) for concat_df_list in self.__timeframe_idx_to_concat_df_list_dict.values()])
        
        if is_all_candle_retrieved:
            #for timeframe, concat_df_list in self.__timeframe_idx_to_concat_df_list_dict.items():
            one = pd.concat(self.__timeframe_idx_to_concat_df_list_dict[0], axis=1)
            five = pd.concat(self.__timeframe_idx_to_concat_df_list_dict[1], axis=1)

    def __get_historical_data_and_analyse(self, timeframe_list: list):
        req_id_multiplier = len(self.__scanner_result_list)
        candle_start_timeframe = str(get_trading_interval() - 60)
        
        self.__timeframe_idx_to_ohlvav_list_dict = {}
        self.__timeframe_idx_to_datetime_list_dict = {}
        self.__timeframe_idx_to_concat_df_list_dict = {}
        
        for timeframe_index, timeframe in enumerate(timeframe_list):
            self.__timeframe_idx_to_ohlvav_list_dict[timeframe_index] = []
            self.__timeframe_idx_to_datetime_list_dict[timeframe_index] = []
            self.__timeframe_idx_to_concat_df_list_dict[timeframe_index] = []

            for index, ticker in enumerate(self.__scanner_result_list, start=1):
                candle_req_id = (timeframe_index * req_id_multiplier) + index
                contract = get_contract(ticker)
                self.reqHistoricalData(candle_req_id, contract, '', candle_start_timeframe, timeframe.value, 'TRADES', 0, 1, False, [])
                logger.debug(f'Action: historicalData, reqId: {candle_req_id}, Timeframe index: {timeframe_index}, Ticker: {ticker}')
    
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        if rank == 0:
            self.__scanner_result_list = []
            self.__timeframe_idx_to_ohlvav_list_dict = {}
            self.__timeframe_idx_to_datetime_list_dict = {}
            self.__timeframe_idx_to_concat_df_list_dict = {}
        
        self.__scanner_result_list.append(contractDetails.contract.symbol)

    #scannerDataEnd marker will indicate when all results have been delivered.
    #The returned results to scannerData simply consists of a list of contracts, no market data field (bid, ask, last, volume, ...).
    def scannerDataEnd(self, reqId: int):
        logger.debug(f'Action: scannerDataEnd, reqId: {reqId}, Result length: {len(self.__scanner_result_list)}, Result: {self.__scanner_result_list}, Previous result length: {len(self.__previous_scanner_result_list)}, Previous result: {self.__previous_scanner_result_list}')
        self.__get_historical_data_and_analyse([Timeframe.ONE_MINUTE, Timeframe.FIVE_MINUTE])