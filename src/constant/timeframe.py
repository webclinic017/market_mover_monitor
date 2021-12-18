from enum import Enum

class Timeframe(str, Enum):
    ONE_MINUTE = '1 min',
    FIVE_MINUTE = '5 mins'