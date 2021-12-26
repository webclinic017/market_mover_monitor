from enum import Enum

class CustomisedIndicator(str, Enum):
    CLOSE_CHANGE = 'Close Change',
    PREVIOUS_CLOSE_CHANGE = 'Previous Close Change',
    CANDLE_COLOUR = 'Candle Colour',
    MARUBOZU_RATIO = 'Marubozu Ratio',
    BODY_DIFF_RATIO = 'Body Diff Ratio',
    VWAP = 'Vwap',
    TOTAL_VOLUME = 'Total Volume',
    MA_20_VOLUME = '20MA Volume',
    MA_50_VOLUME = '50MA Volume'