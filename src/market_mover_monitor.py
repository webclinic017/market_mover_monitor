import os

from exception.connection_exception import ConnectionException

from constant.filter.scan_code import ScanCode
from constant.instrument import Instrument

from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine
from utils.file_util import clean_txt_file_content
from utils.filter_util import get_filter

from datasource.ib_connector import IBConnector

logger = get_logger(console_log=True)
text_to_speech_engine = get_text_to_speech_engine()

def main():
    try:
        connector = None
        connector = IBConnector()
        connector.connect('127.0.0.1', 7496, 0)

        filter = get_filter(scan_code = ScanCode.TOP_GAINERS.value, instrument = Instrument.STOCKS.value, 
                            min_price = 0.3, min_volume = 10000, 
                            include_otc = False,
                            no_of_result = 20)

        #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
        print('Listening...')
        connector.reqScannerSubscription(0, filter, [], [])
        connector.run()
    except Exception as e:
        if connector:
            connector.disconnect()

        is_connection_exception = True if isinstance(e, ConnectionException) else False
        display_msg = 'TWS API Connection Lost' if is_connection_exception else 'Fatal Error'
        read_msg = 'Re-establishing Connection Due to Connectivity Issue' if is_connection_exception else 'Close Connection Due to Fatal Error'

        os.system('cls')
        logger.exception(f'{display_msg}, Cause: {e}')
        
        text_to_speech_engine.say(f'{read_msg}')
        text_to_speech_engine.runAndWait()

        if is_connection_exception:
            main()

if __name__ == '__main__':
    log_dir = 'log.txt'
    clean_txt_file_content(log_dir)
    main()