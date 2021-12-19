from pandas.core.frame import DataFrame

from constant.filter.pattern import Pattern
from pattern.pattern_analyser import PatternAnalyser
from pattern.unusual_ramp_up import UnusualRampUp

class PatternAnalyserFactory:
    @staticmethod
    def get_pattern_analyser(analyser: str, historical_data_df: DataFrame) -> PatternAnalyser:
        if Pattern.UNUSUAL_RAMP_UP == analyser:
            return UnusualRampUp(historical_data_df)
        else:
            raise Exception(f'Pattern analyser of {analyser} not found')