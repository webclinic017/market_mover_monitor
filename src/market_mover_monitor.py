from ibapi.client import EClient
from ibapi.contract import Contract, ContractDetails
from ibapi.scanner import ScannerSubscription
from ibapi.wrapper import EWrapper
import time

class IBConnector(EWrapper, EClient):
    def __init__(self):
        #Callbacks to EWrapper with errorId as -1 do not represent true 'errors' but only notifications that a connection has been made successfully to the IB market data farms.
        #EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.__data = []

    def historicalData(self, reqId, bar):
        #print(f'Time: {bar.date} Close: {bar.close} Volume: {bar.volume}',reqId)
        self.__data.append([bar.date, bar.close, bar.volume, reqId])
    
    #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark, projection, legsStr)
        print(f'Scanner Data ReqId: {reqId}, {contractDetails.contract}, Rank: {rank}')

def main():
    connector = IBConnector()
    connector.connect('127.0.0.1', 7496, 0)
    print('after run')

    scannerFilter = ScannerSubscription()
    scannerFilter.scanCode = 'TOP_PERC_GAIN'
    scannerFilter.instrument = 'STK'
    scannerFilter.locationCode = 'STK.US.MAJOR' 
    scannerFilter.abovePrice = 0.3
    scannerFilter.aboveVolume = 10000
    scannerFilter.numberOfRows = 20
    
    connector.reqScannerSubscription(1, scannerFilter, [], [])

    '''contract = Contract()
    contract.symbol = 'TSLA'
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    connector.reqHistoricalData(1, contract, '', "10 D", "1 day", "TRADES", 1, 1, False, [])'''

    connector.run()

main()