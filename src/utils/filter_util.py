from ibapi.scanner import ScannerSubscription
from ibapi.contract import Contract

from constant.instrument import Instrument
from constant.filter.scan_code import ScanCode

def get_filter(
            scan_code: ScanCode, instrument: Instrument, 
            min_price: float, min_volume: int, 
            include_otc: bool,
            no_of_result: int) -> ScannerSubscription:
    scannerFilter = ScannerSubscription()

    scannerFilter.scanCode = scan_code
    scannerFilter.instrument = instrument

    if not include_otc:
        scannerFilter.locationCode = 'STK.US.MAJOR'
    else:
        scannerFilter.locationCode = 'STK.US'

    if min_price:
        scannerFilter.abovePrice = min_price
        
    if min_volume:
        scannerFilter.aboveVolume = min_volume
    
    #Maximum no. of rows is 50, no_of_result shouldn't exceed 50
    scannerFilter.numberOfRows = no_of_result

    return scannerFilter