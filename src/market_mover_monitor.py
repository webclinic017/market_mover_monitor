from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import ContractDetails
from ibapi.wrapper import EWrapper

import pandas as pd
from pytz import timezone
import time
import os

from constant.indicator import Indicator
from constant.scan_code import ScanCode
from constant.instrument import Instrument

from utils.file_util import clean_txt_file_content
from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine
from utils.datetime_util import convert_datetime_format_str, get_trading_interval
from utils.filter_util import get_filter, get_contract

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

idx = pd.IndexSlice

#Add Detailed Log Message(Retrieved/ Constructed Data, Time Spent)
#Optimise Workflow(Minimise Asynchronous Request, Organise Callbacks)

class IBConnector(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.__previous_scanner_result_list = []
        self.__scanner_result_list = []

        self.__previous_day_ohlcav_list = []
        self.__previous_day_timeframe_list = []
        self.__previous_day_candle_df_list = []

        self.__one_minute_ohlcav_list = []
        self.__one_minute_timeframe_list = []
        self.__one_minute_candle_df_list = []

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
        rank = (reqId % len(self.__scanner_result_list) - 1) if (reqId % len(self.__scanner_result_list) != 0) else len(self.__scanner_result_list) - 1
        is_previous_day_candle_req = (reqId < len(self.__scanner_result_list))
        is_one_minute_candle_req = (reqId >= len(self.__scanner_result_list))
        
        open = bar.open
        high = bar.high
        low = bar.low
        close = bar.close
        avg = bar.wap
        volume = bar.volume * 100
        dt = bar.date

        if is_previous_day_candle_req:
            self.__previous_day_ohlcav_list.append([open, high, low, close, avg, volume])
            self.__previous_day_timeframe_list.append(dt)
        elif is_one_minute_candle_req:
            self.__one_minute_ohlcav_list.append([open, high, low, close, avg, volume])
            self.__one_minute_timeframe_list.append(dt)

        logger.debug(f'Action: historicalData, reqId: {reqId}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}, Bar datetime: {convert_datetime_format_str(dt, "%Y%m%d", "%Y-%m-%d")}, Open: {open}, High: {high}, Low: {low}, Close: {close}, Avgerage: {avg}, Volume: {volume}')

    #Marks the ending of historical bars reception
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        rank = (reqId % len(self.__scanner_result_list) - 1) if (reqId % len(self.__scanner_result_list) != 0) else len(self.__scanner_result_list) - 1
        is_previous_day_candle_req = (reqId < len(self.__scanner_result_list))
        is_one_minute_candle_req = (reqId >= len(self.__scanner_result_list))
        
        ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__scanner_result_list[rank]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.BAR_AVERAGE, Indicator.VOLUME]])
        
        if is_previous_day_candle_req:
            logger.debug(f'Action: historicalDataEnd, reqId: {reqId}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}, Bar start time: {convert_datetime_format_str(self.__previous_day_timeframe_list[rank], "%Y%m%d", "%Y-%m-%d")}, Bar end time: {convert_datetime_format_str(self.__previous_day_timeframe_list[-1], "%Y%m%d", "%Y-%m-%d")}')
            datetime_index = pd.DatetimeIndex(self.__previous_day_timeframe_list)
            candle_df = pd.DataFrame(self.__previous_day_ohlcav_list, columns=ticker_to_indicator_column, index=datetime_index)
            self.__previous_day_candle_df_list.append(candle_df)
        elif is_one_minute_candle_req:
            logger.debug(f'Action: historicalDataEnd, reqId: {reqId}, Rank: {rank}, Ticker: {self.__scanner_result_list[rank]}, Bar start time: {convert_datetime_format_str(self.__one_minute_timeframe_list[rank])}, Bar end time: {convert_datetime_format_str(self.__one_minute_timeframe_list[-1])}')
            datetime_index = pd.DatetimeIndex(self.__one_minute_timeframe_list)
            candle_df = pd.DataFrame(self.__one_minute_ohlcav_list, columns=ticker_to_indicator_column, index=datetime_index)
            self.__one_minute_candle_df_list.append(candle_df)

        if len(self.__previous_day_candle_df_list) == len(self.__scanner_result_list):
            self.__previous_day_historical_data_df = pd.concat(self.__previous_day_candle_df_list, axis=1)
            logger.debug('Full previous day historical DataFrame: \n' + self.__previous_day_historical_data_df.to_string().replace('\n', '\n\t'))
        elif len(self.__one_minute_candle_df_list) == len(self.__scanner_result_list):
            self.__previous_day_historical_data_df = pd.concat(self.__one_minute_candle_df_list, axis=1)
            logger.debug('Full 1 minute candle historical DataFrame: \n' + self.__previous_day_historical_data_df.to_string().replace('\n', '\n\t'))
        
        if (len(self.__previous_day_candle_df_list) == len(self.__scanner_result_list) 
                and len(self.__one_minute_candle_df_list) == len(self.__scanner_result_list)):
            self.__analyse()
            
            #Set previous scanner result as current scanner result for comparison to that of the next iteration
            self.__previous_scanner_result_list = self.__scanner_result_list

            #Clear scanner result and historical data after analysis for the next iteration
            self.__previous_day_candle_df_list = []
            self.__one_minute_candle_df_list = []
            self.__scanner_result_list = []  
            logger.debug(f'--- Total scanning and analysis time: {time.time() - self.__start_time} seconds ---')

    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        if rank == 0:
            self.__start_time = time.time()

        self.__scanner_result_list.append(contractDetails.contract.symbol)

    #scannerDataEnd marker will indicate when all results have been delivered
    #The returned results to scannerData simply consists of a list of contracts, no market data field (bid, ask, last, volume, ...)
    def scannerDataEnd(self, reqId: int):
        logger.debug(f'Action: scannerDataEnd, reqId: {reqId}, Ticker list length: {len(self.__scanner_result_list)}, Result: {self.__scanner_result_list}')

        for index, ticker in enumerate(self.__scanner_result_list):
            contract = get_contract(ticker)
            self.reqHistoricalData(index + 1, contract, '', '1 D', '1 day', 'TRADES', 1, 1, False, [])
  
            interval = get_trading_interval(timezone('US/Eastern'))
            self.reqHistoricalData((len(self.__scanner_result_list) + index + 1), contract, '', str(interval), '1 day', 'TRADES', 1, 1, False, [])

    def __analyse(self):
        new_ticker_list = list(set(self.__scanner_result_list) - set(self.__previous_scanner_result_list)) if len(self.__previous_scanner_result_list) > 0 else []
        os.system('cls')

        for ticker in self.__scanner_result_list:
            pct_change = self.__historical_data_df.loc[:, idx[ticker, Indicator.CLOSE]].pct_change().mul(100).iloc[[-1]].values[0]
            last_volume = self.__historical_data_df.loc[:, idx[ticker, Indicator.VOLUME]].iloc[[-1]].values[0]
            print(f'{ticker}, {pct_change}%, {last_volume}')
            
            if ticker in new_ticker_list:
                text_to_speech_engine.say(f'{ticker}, {pct_change} percent up, Volume: {last_volume}')
                text_to_speech_engine.runAndWait()
        
        if len(new_ticker_list) > 0:
            print(f'Latest Gainers: {new_ticker_list}')
    
    '''
    def fundamentalData(self, reqId: TickerId, data: str):
        print(data)
    
        self.reqFundamentalData(self, reqId:TickerId , contract:Contract,
                                reportType:str, fundamentalDataOptions:TagValueList)
    '''

def main():
    try:
        log_dir = 'log.txt'
        clean_txt_file_content(log_dir)

        connector = IBConnector()
        connector.connect('127.0.0.1', 7496, 0)

        filter = get_filter(scan_code = ScanCode.TOP_GAINERS.value, instrument = Instrument.STOCKS.value, 
                            min_price = 0.3, min_volume = 10000, 
                            include_otc = False,
                            max_rank = 20)

        #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
        connector.reqScannerSubscription(0, filter, [], [])
        connector.run()
    except Exception as e:
        connector.disconnect()
        print(f'Error occurs, Cause: {e}')
        logger.exception(e)

if __name__ == '__main__':
    main()