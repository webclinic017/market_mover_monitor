import os
import time

from exception.connection_exception import ConnectionException
from exception.after_hour_reset_exception import AfterHourResetException

from constant.filter.scan_code import ScanCode
from constant.instrument import Instrument

from utils.log_util import get_logger
from utils.file_util import clean_txt_file_content
from utils.filter_util import get_filter
from utils.datetime_util import get_current_datetime, is_normal_trading_hours, is_postmarket_hours, is_premarket_hours

from model.text_to_speech_engine import TextToSpeechEngine

from datasource.ib_connector import IBConnector

logger = get_logger(console_log=True)
text_to_speech_engine = TextToSpeechEngine()

def main():
    is_idle_msg_print = False
    connector = None

    try:
        while True:
            current_datetime = get_current_datetime()

            #Ensure scanner is running in trading hours
            if is_premarket_hours(current_datetime) or is_normal_trading_hours(current_datetime) or is_postmarket_hours(current_datetime):
                
                if is_premarket_hours(current_datetime) or is_normal_trading_hours(current_datetime):
                    scan_code = ScanCode.TOP_GAINERS.value
                    is_after_hour = False
                elif is_postmarket_hours(current_datetime):
                    scan_code = ScanCode.TOP_GAINERS_IN_AFTER_HOURS.value
                    is_after_hour = True

                print('Listening...')
                text_to_speech_engine.speak('Connecting')

                connector = IBConnector(is_after_hour)
                connector.connect('127.0.0.1', 7496, 0)

                #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
                filter = get_filter(scan_code = scan_code, instrument = Instrument.STOCKS.value, 
                                min_price = 0.3, min_volume = 10000, 
                                include_otc = False,
                                no_of_result = 25)
                connector.reqScannerSubscription(0, filter, [], [])
                connector.run()
            elif not is_idle_msg_print:
                print('Scanner is idle till valid trading hours...')
                text_to_speech_engine.speak('Scanner is idle')
                is_idle_msg_print = True
    except Exception as e:
        if connector:
            connector.disconnect()

        if isinstance(e, ConnectionException):
            sleep_time = 80

            os.system('cls')
            logger.exception(f'TWS API Connection Lost, Cause: {e}')
            text_to_speech_engine.speak('Re-establishing Connection Due to Connectivity Issue')

        elif isinstance(e, AfterHourResetException):
            sleep_time = None

            logger.debug('Reset Scanner Scan Code for After Hours')
            text_to_speech_engine.speak('After Hour Scanner Reset')
        else:
            sleep_time = 10

            os.system('cls')
            logger.exception(f'Fatal Error, Cause: {e}')
            text_to_speech_engine.speak('Re-establishing Connection Due to Fatal Error')
        
        if sleep_time:
            time.sleep(sleep_time)

        main()

if __name__ == '__main__':
    log_dir = 'log.txt'
    clean_txt_file_content(log_dir)
    main()