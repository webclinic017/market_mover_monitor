from constant.filter.scan_code import ScanCode
from constant.instrument import Instrument

from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine
from utils.file_util import clean_txt_file_content
from utils.filter_util import get_filter

from datasource.ib_connector import IBConnector

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

def main():
    try:
        connector = IBConnector()
        connector.connect('127.0.0.1', 7496, 0)

        filter = get_filter(scan_code = ScanCode.TOP_GAINERS.value, instrument = Instrument.STOCKS.value, 
                            min_price = 0.3, min_volume = 10000, 
                            include_otc = False,
                            no_of_result = 20)

        #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
        connector.reqScannerSubscription(0, filter, [], [])
        connector.run()
    except Exception as e:
        logger.exception(e)
        print(f'Error occurs, Cause: {e}')
        print('Restart program')
        text_to_speech_engine.say(f'Error occurs due to {e}, re-establishing connection')
        text_to_speech_engine.runAndWait()
        connector.disconnect()
        main()

if __name__ == '__main__':
    log_dir = 'log.txt'
    clean_txt_file_content(log_dir)
    main()