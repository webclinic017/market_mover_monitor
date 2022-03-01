from enum import Enum

class ScanCode(str, Enum):
    TOP_GAINERS = 'TOP_PERC_GAIN',
    TOP_GAINERS_IN_AFTER_HOURS = 'TOP_AFTER_HOURS_PERC_GAIN',
    TOP_LOSERS = 'TOP_PERC_LOSE'