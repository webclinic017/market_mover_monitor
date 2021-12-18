from enum import Enum

class CustomisedIndicator(str, Enum):
    VWAP = 'VWAP',
    EMA_9 = 'EMA_9',
    EMA_20 = 'EMA_20'