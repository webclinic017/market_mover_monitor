from ibapi.scanner import ScannerSubscription
from ibapi.contract import Contract

from constant.instrument import Instrument
from constant.scan_code import ScanCode

def get_filter(
            scan_code: ScanCode, instrument: Instrument, 
            min_price: float, min_volume: int, 
            include_otc: bool,
            max_rank: int) -> ScannerSubscription:
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
    
    scannerFilter.numberOfRows = max_rank

    return scannerFilter

def get_contract(ticker: str) -> Contract:
    contract = Contract()
    contract.symbol = ticker
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    
    return contract