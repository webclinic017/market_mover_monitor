from constant.scan_code import ScanCode
from constant.instrument import Instrument

from utils.file_util import clean_txt_file_content
from utils.log_util import get_logger
from utils.filter_util import get_filter

from datasource.ib_connector import IBConnector

logger = get_logger(console_log=False)

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
        raise e

if __name__ == '__main__':
    main()