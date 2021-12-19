from pandas.core.frame import DataFrame
import pandas as pd
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.indicator import Indicator

from pattern.pattern_analyser import PatternAnalyser
from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

idx = pd.IndexSlice

class UnusualRampUp(PatternAnalyser):
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        latest_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].iloc[[-1]]
        #latest_vol_df = self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME]].iloc[[-1]]

        max_idx_df = pd.DataFrame(latest_close_pct_df.reset_index(drop=True).idxmax()).T
        result_boolean_df = (max_idx_df == len(latest_close_pct_df))

        ticker_to_filtered_result_series = result_boolean_df.any()
        ticker_list = ticker_to_filtered_result_series.index[ticker_to_filtered_result_series].get_level_values(0).tolist()

        for ticker in ticker_list:
            pop_up_pct = latest_close_pct_df.loc[:, idx[ticker, :]].values[0][0]
            #logger.debug(f'{ticker} is popping up {pop_up_pct}')
            text_to_speech_engine.say(f'{ticker} is popping up {pop_up_pct}')
            text_to_speech_engine.runAndWait()