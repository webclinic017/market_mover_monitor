from enum import Enum

class Indicator(str, Enum):
    OPEN = 'Open',
    HIGH = 'High',
    LOW = 'Low',
    CLOSE = 'Close',
    BAR_AVERAGE = 'Average',
    VOLUME = 'Volume'