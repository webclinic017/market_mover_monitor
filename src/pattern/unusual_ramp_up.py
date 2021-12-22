from pandas.core.frame import DataFrame
import pandas as pd

from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.indicator import Indicator
from constant.indicator.runtime_indicator import RuntimeIndicator

from pattern.pattern_analyser import PatternAnalyser

from utils.log_util import get_logger
from utils.text_to_speech_util import get_text_to_speech_engine
from utils.dataframe_util import derive_idx_df

logger = get_logger(console_log=False)
text_to_speech_engine = get_text_to_speech_engine()

idx = pd.IndexSlice

class UnusualRampUp(PatternAnalyser):
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        previous_close_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE]]
        close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
        previous_close_pct_df = ((close_df.sub(previous_close_df.values)
                                            .div(previous_close_df.values))
                                            .mul(100)).rename(columns={Indicator.CLOSE: CustomisedIndicator.PREVIOUS_CLOSE_CHANGE})
        latest_close_df = close_df.iloc[[-1]]
        
        #New Top Gainer Popping Up
        numeric_idx_df = derive_idx_df(previous_close_pct_df).rename(columns={RuntimeIndicator.INDEX: RuntimeIndicator.COMPARE})
        ramp_up_boolean_df = (previous_close_pct_df >= 6.5)
        first_occurrence_ramp_up_idx_df = numeric_idx_df.where(ramp_up_boolean_df.values).bfill().iloc[[0]]
        new_gainer_boolean_df = (first_occurrence_ramp_up_idx_df == (len(previous_close_pct_df) - 1))

        new_gainer_result_series = new_gainer_boolean_df.any()
        new_gainer_ticker_list = new_gainer_result_series.index[new_gainer_result_series].get_level_values(0).tolist()

        #Unusual Ramp Up(For Existing Top Gainers Only)
        latest_vol_df = self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME]].iloc[[-1]].rename(columns={Indicator.VOLUME: RuntimeIndicator.COMPARE})
        latest_vol_20_ma_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MA_20_VOLUME]].iloc[[-1]].rename(columns={CustomisedIndicator.MA_20_VOLUME: RuntimeIndicator.COMPARE})
        latest_vol_50_ma_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MA_50_VOLUME]].iloc[[-1]].rename(columns={CustomisedIndicator.MA_50_VOLUME: RuntimeIndicator.COMPARE})
        latest_candle_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].iloc[[-1]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})

        above_vol_20_ma_boolean_df = (latest_vol_df >= latest_vol_20_ma_df) & (latest_vol_df >= 3000)
        above_vol_50_ma_boolean_df = (latest_vol_df >= latest_vol_50_ma_df) & (latest_vol_df >= 3000)
        latest_candle_close_pct_boolean_df = (latest_candle_close_pct_df > 2)
        above_vol_20_ma_ramp_up_boolean_df = (above_vol_20_ma_boolean_df) & (latest_candle_close_pct_boolean_df) & (~new_gainer_boolean_df.set_index(latest_candle_close_pct_boolean_df.index))
        above_vol_50_ma_ramp_up_boolean_df = (above_vol_50_ma_boolean_df) & (latest_candle_close_pct_boolean_df) & (~new_gainer_boolean_df.set_index(latest_candle_close_pct_boolean_df.index))

        above_vol_20_ma_result_series = above_vol_20_ma_ramp_up_boolean_df.any()
        above_vol_20_ma_ticker_list = above_vol_20_ma_result_series.index[above_vol_20_ma_result_series].get_level_values(0).tolist()
        above_vol_50_ma_result_series = above_vol_50_ma_ramp_up_boolean_df.any()
        above_vol_50_ma_ticker_list = above_vol_50_ma_result_series.index[above_vol_50_ma_result_series].get_level_values(0).tolist()
        above_vol_20_ma_ticker_list = [ticker for ticker in above_vol_20_ma_ticker_list if ticker not in above_vol_50_ma_ticker_list]

        #logger.debug('Full historical DataFrame: \n' + self.__historical_data_df.loc[:, idx[:, [Indicator.CLOSE, CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.PREVIOUS_CLOSE, Indicator.VOLUME, CustomisedIndicator.MA_20_VOLUME, CustomisedIndicator.MA_50_VOLUME]]].to_string().replace('\n', '\n\t'))
        
        if (len(new_gainer_ticker_list) 
                or (len(above_vol_20_ma_ticker_list)) 
                or (len(above_vol_50_ma_ticker_list))):
            logger.debug('Full historical DataFrame: \n' + self.__historical_data_df.loc[:, idx[:, [Indicator.CLOSE, CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.PREVIOUS_CLOSE, Indicator.VOLUME, CustomisedIndicator.MA_20_VOLUME, CustomisedIndicator.MA_50_VOLUME]]].to_string().replace('\n', '\n\t'))
        
        for ticker in new_gainer_ticker_list:
            close = latest_close_df.loc[:, idx[ticker, :]].values[0][0]
            pop_up_pct = previous_close_pct_df.loc[:, idx[ticker, :]].iloc[[-1]].values[0][0]
            round_pop_up_pct = round(pop_up_pct, 2)
            pop_up_hour = latest_close_df.index.tolist()[0].hour
            pop_up_minute = latest_close_df.index.tolist()[0].minute
            logger.debug(f'{ticker} is popping up {round_pop_up_pct}% at {pop_up_hour} {pop_up_minute}, Close: {close}')
            print(f'{ticker} is popping up {round_pop_up_pct}% at {pop_up_hour} {pop_up_minute}, Close: {close}')
            text_to_speech_engine.say(f'{ticker} is popping up {round_pop_up_pct} percent at {pop_up_hour} {pop_up_minute}')
            text_to_speech_engine.runAndWait()
        
        for ticker in above_vol_20_ma_ticker_list:
            close = latest_close_df.loc[:, idx[ticker, :]].values[0][0]
            ramp_up_pct = latest_candle_close_pct_df.loc[:, idx[ticker, :]].values[0][0]
            round_ramp_up_pct = round(ramp_up_pct, 2)
            pop_up_hour = latest_candle_close_pct_df.index.tolist()[0].hour
            pop_up_minute = latest_candle_close_pct_df.index.tolist()[0].minute
            vol_20_ma = latest_vol_20_ma_df.loc[:, idx[ticker, :]].values[0][0]
            logger.debug(f'{ticker} is ramping up {round_ramp_up_pct}% above 20 MA volume at {pop_up_hour} {pop_up_minute}, 20MA volume: {vol_20_ma}, Close: {close}')
            print(f'{ticker} is ramping up {round_ramp_up_pct}% above 20MA volume at {pop_up_hour} {pop_up_minute}, 20MA volume: {vol_20_ma}, Close: {close}')
            text_to_speech_engine.say(f'{ticker} is ramping up {round_ramp_up_pct} percent above 20 M A volume at {pop_up_hour} {pop_up_minute}')
            text_to_speech_engine.runAndWait()

        for ticker in above_vol_50_ma_ticker_list:
            close = latest_close_df.loc[:, idx[ticker, :]].values[0][0]
            ramp_up_pct = latest_candle_close_pct_df.loc[:, idx[ticker, :]].values[0][0]
            round_ramp_up_pct = round(ramp_up_pct, 2)
            pop_up_hour = latest_candle_close_pct_df.index.tolist()[0].hour
            pop_up_minute = latest_candle_close_pct_df.index.tolist()[0].minute
            vol_50_ma = latest_vol_50_ma_df.loc[:, idx[ticker, :]].values[0][0]
            logger.debug(f'{ticker} is ramping up {round_ramp_up_pct}% above 50MA volume at {pop_up_hour} {pop_up_minute}, 50MA volume: {vol_50_ma}, Close: {close}')
            print(f'{ticker} is ramping up {round_ramp_up_pct}% above 50MA volume at {pop_up_hour} {pop_up_minute}, 50MA volume: {vol_50_ma}, Close: {close}')
            text_to_speech_engine.say(f'{ticker} is ramping up {round_ramp_up_pct} percent above 50 M A volume at {pop_up_hour} {pop_up_minute}')
            text_to_speech_engine.runAndWait()
        
