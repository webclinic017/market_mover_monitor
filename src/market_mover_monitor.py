import os
import time

from exception.connection_exception import ConnectionException

from constant.filter.scan_code import ScanCode
from constant.instrument import Instrument
from utils.datetime_util import get_current_datetime, is_normal_trading_hours, is_postmarket_hours, is_premarket_hours

from utils.log_util import get_logger
from model.text_to_speech_engine import TextToSpeechEngine
from utils.file_util import clean_txt_file_content
from utils.filter_util import get_filter

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
                print('Listening...')
                text_to_speech_engine.speak('Connecting')

                connector = IBConnector()
                connector.connect('127.0.0.1', 7496, 0)

                #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
                filter = get_filter(scan_code = ScanCode.TOP_GAINERS.value, instrument = Instrument.STOCKS.value, 
                                min_price = 0.3, min_volume = 10000, 
                                include_otc = False,
                                no_of_result = 20)

                connector.reqScannerSubscription(0, filter, [], [])
                connector.run()
            elif not is_idle_msg_print:
                print('Scanner is idle till valid trading hours...')
                text_to_speech_engine.speak('Scanner is idle')
                is_idle_msg_print = True
    except Exception as e:
        if connector:
            connector.disconnect()

        is_connection_exception = True if isinstance(e, ConnectionException) else False
        display_msg = 'TWS API Connection Lost' if is_connection_exception else 'Fatal Error'
        read_msg = 'Re-establishing Connection Due to Connectivity Issue' if is_connection_exception else 'Re-establishing Connection Due to Fatal Error'
        sleep_time = 80 if is_connection_exception else 10

        os.system('cls')
        text_to_speech_engine.speak(f'{read_msg}')
        logger.exception(f'{display_msg}, Cause: {e}')

        time.sleep(sleep_time)
        main()

if __name__ == '__main__':
    log_dir = 'log.txt'
    clean_txt_file_content(log_dir)
    main()