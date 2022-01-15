from pandas.core.frame import DataFrame
import pandas as pd

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from pattern.pattern_analyser import PatternAnalyser

from utils.log_util import get_logger
from model.text_to_speech_engine import TextToSpeechEngine
from utils.dataframe_util import derive_idx_df

logger = get_logger(console_log=False)
text_to_speech_engine = TextToSpeechEngine()

idx = pd.IndexSlice

class InitialPopUp(PatternAnalyser):
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        min_marubozu_ratio = 40
        min_close_pct = 4.6
        min_previous_close_pct = 6
        max_ramp_occurrence = 5
        notify_period = 3

        candle_colour_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR]].rename(columns={CustomisedIndicator.CANDLE_COLOUR: RuntimeIndicator.COMPARE})
        marubozu_ratio_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MARUBOZU_RATIO]].rename(columns={CustomisedIndicator.MARUBOZU_RATIO: RuntimeIndicator.COMPARE})
        close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        previous_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]].rename(columns={CustomisedIndicator.PREVIOUS_CLOSE_CHANGE: RuntimeIndicator.COMPARE})

        green_candle_df = (candle_colour_df == CandleColour.GREEN)
        marubozu_boolean_df = (marubozu_ratio_df >= min_marubozu_ratio)
        candle_close_pct_boolean_df = (close_pct_df >= min_close_pct)
        previous_close_pct_boolean_df = (previous_close_pct_df >= min_previous_close_pct)

        ramp_up_boolean_df = (green_candle_df) & (marubozu_boolean_df) & (candle_close_pct_boolean_df) & (previous_close_pct_boolean_df)
        ramp_up_occurrence_df = ((ramp_up_boolean_df.cumsum()
                                                    .where(ramp_up_boolean_df.values)))
        result_boolean_df = (ramp_up_occurrence_df.iloc[-notify_period:] <= max_ramp_occurrence)
        new_gainer_result_series = result_boolean_df.any()   
        new_gainer_ticker_list = new_gainer_result_series.index[new_gainer_result_series].get_level_values(0).tolist()

        if len(new_gainer_ticker_list) > 0:
            logger.debug('Ramp Up Boolean DataFrame: \n' + ramp_up_boolean_df.to_string().replace('\n', '\n\t'))
            logger.debug('Ramp Up Occurrence DataFrame: \n' + ramp_up_occurrence_df.to_string().replace('\n', '\n\t'))
            logger.debug('Result DataFrame: \n' + result_boolean_df.to_string().replace('\n', '\n\t'))
            logger.debug(f'Candle DataFrame Timeframe Length: {len(previous_close_pct_df)}')
            logger.debug('Full initial pop up historical DataFrame: \n' + self.__historical_data_df.loc[:, idx[:, [CustomisedIndicator.CANDLE_COLOUR, CustomisedIndicator.MARUBOZU_RATIO, Indicator.OPEN, Indicator.CLOSE, CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]]].to_string().replace('\n', '\n\t'))

            datetime_idx_df = derive_idx_df(ramp_up_occurrence_df, numeric_idx=False)
            close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
            previous_close_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]]
            volume_df = self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME]]
            
            pop_up_close_df = close_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_close_pct_df = close_pct_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_previous_close_pct_df = previous_close_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_volume_df = volume_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_datetime_idx_df = datetime_idx_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]

            for ticker in new_gainer_ticker_list:
                display_close = pop_up_close_df.loc[:, ticker].iat[0, 0]
                display_volume = pop_up_volume_df.loc[:, ticker].iat[0, 0]
                display_close_pct = round(pop_up_close_pct_df.loc[:, ticker].iat[0, 0], 2)
                display_previous_close_pct = round(pop_up_previous_close_pct_df.loc[:, ticker].iat[0, 0], 2)

                pop_up_datetime = pop_up_datetime_idx_df.loc[:, ticker].iat[0, 0]
                pop_up_hour = pd.to_datetime(pop_up_datetime).hour
                pop_up_minute = pd.to_datetime(pop_up_datetime).minute
                display_hour = ('0' + str(pop_up_hour)) if pop_up_hour < 10 else pop_up_hour
                display_minute = ('0' + str(pop_up_minute)) if pop_up_minute < 10 else pop_up_minute
                display_time_str = f'{display_hour}:{display_minute}'
                read_time_str = f'{pop_up_hour} {pop_up_minute}' if (pop_up_minute > 0) else f'{pop_up_hour} o clock' 
                read_ticker_str = " ".join(ticker)

                logger.debug(f'{ticker} is popping up {display_previous_close_pct}%, Time: {display_time_str}, Close: ${display_close}, Change: {display_close_pct}%, Volume: {display_volume}')
                print(f'{ticker} is popping up {display_previous_close_pct}%, Time: {display_time_str}, Close: ${display_close}, Change: {display_close_pct}%, Volume: {display_volume}')
                text_to_speech_engine.speak(f'{read_ticker_str} is popping up {display_previous_close_pct} percent at {read_time_str}')