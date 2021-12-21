from enum import Enum

class CustomisedIndicator(str, Enum):
    CLOSE_CHANGE = 'Close Change'
    VWAP = 'Vwap',
    PREVIOUS_CLOSE = 'Previous Close',
    PREVIOUS_CLOSE_CHANGE = 'Previous Close Change',
    MA_20_VOLUME = '20MA Volume',
    MA_50_VOLUME = '50MA Volume',
    TOTAL_VOLUME = 'Total Volume'