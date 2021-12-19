from enum import Enum

class CustomisedIndicator(str, Enum):
    VWAP = 'Vwap',
    PREVIOUS_CLOSE = 'Previous Close',
    CLOSE_CHANGE = 'Close Change'
    MA_20_VOLUME = '20MA Volume',
    MA_50_VOLUME = '50MA Volume',
    TOTAL_VOLUME = 'Total Volume'