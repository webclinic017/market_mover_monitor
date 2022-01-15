from pandas.core.frame import DataFrame
import pandas as pd
from constant.candle.candle_colour import CandleColour

from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.indicator import Indicator
from constant.indicator.runtime_indicator import RuntimeIndicator

from pattern.pattern_analyser import PatternAnalyser

from utils.log_util import get_logger
from model.text_to_speech_engine import TextToSpeechEngine
from utils.dataframe_util import derive_idx_df

logger = get_logger(console_log=False)
text_to_speech_engine = TextToSpeechEngine()

idx = pd.IndexSlice

class UnusualVolumeRampUp(PatternAnalyser):
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        min_marubozu_ratio = 40
        min_close_pct = 2
        min_volume = 3000
        notify_period = 3

        close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        candle_colour_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR]].rename(columns={CustomisedIndicator.CANDLE_COLOUR: RuntimeIndicator.COMPARE})
        marubozu_ratio_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MARUBOZU_RATIO]].rename(columns={CustomisedIndicator.MARUBOZU_RATIO: RuntimeIndicator.COMPARE})
        volume_df = self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME]].rename(columns={Indicator.VOLUME: RuntimeIndicator.COMPARE})
        vol_20_ma_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MA_20_VOLUME]].rename(columns={CustomisedIndicator.MA_20_VOLUME: RuntimeIndicator.COMPARE})
        vol_50_ma_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.MA_50_VOLUME]].rename(columns={CustomisedIndicator.MA_50_VOLUME: RuntimeIndicator.COMPARE})
        
        green_candle_df = (candle_colour_df == CandleColour.GREEN)
        marubozu_boolean_df = (marubozu_ratio_df >= min_marubozu_ratio)
        candle_close_pct_boolean_df = (close_pct_df >= min_close_pct)
        ramp_up_boolean_df = (green_candle_df) & (marubozu_boolean_df) & (candle_close_pct_boolean_df)
        above_vol_20_ma_boolean_df = (volume_df >= vol_20_ma_df) & (vol_20_ma_df >= min_volume) & (ramp_up_boolean_df)
        above_vol_50_ma_boolean_df = (volume_df >= vol_50_ma_df) & (vol_50_ma_df >= min_volume) & (ramp_up_boolean_df)

        above_vol_20_ma_result_boolean_df = above_vol_20_ma_boolean_df.iloc[-notify_period:]
        above_vol_20_ma_result_series = above_vol_20_ma_result_boolean_df.any()
        above_vol_20_ma_ticker_list = above_vol_20_ma_result_series.index[above_vol_20_ma_result_series].get_level_values(0).tolist()
        
        above_vol_50_ma_result_boolean_df = above_vol_50_ma_boolean_df.iloc[-notify_period:]
        above_vol_50_ma_result_series = above_vol_50_ma_result_boolean_df.any()
        above_vol_50_ma_ticker_list = above_vol_50_ma_result_series.index[above_vol_50_ma_result_series].get_level_values(0).tolist()

        above_vol_20_ma_ticker_list = [ticker for ticker in above_vol_20_ma_ticker_list if ticker not in above_vol_50_ma_ticker_list]
        
        if len(above_vol_20_ma_ticker_list) > 0 or len(above_vol_50_ma_ticker_list) > 0:
            logger.debug('Full historical DataFrame: \n' + self.__historical_data_df.loc[:, idx[:, [Indicator.VOLUME, CustomisedIndicator.CLOSE_CHANGE, CustomisedIndicator.MA_20_VOLUME, CustomisedIndicator.MA_50_VOLUME, CustomisedIndicator.CANDLE_COLOUR, CustomisedIndicator.MARUBOZU_RATIO]]].to_string().replace('\n', '\n\t'))
            result_ticker_list = [above_vol_20_ma_ticker_list, above_vol_50_ma_ticker_list]

            for list_idx, ticker_list in enumerate(result_ticker_list):
                if len(ticker_list) > 0:
                    ma_val = '20' if (list_idx == 0) else '50'
                    above_ma_df = above_vol_20_ma_boolean_df if (list_idx == 0) else above_vol_50_ma_boolean_df
                    ma_vol_df = vol_20_ma_df if (list_idx == 0) else vol_50_ma_df
                    
                    logger.debug('Ramp Up Boolean DataFrame: \n' + ramp_up_boolean_df.to_string().replace('\n', '\n\t'))
                    logger.debug('Above {ma_val} MA Boolean DataFrame: \n' + above_ma_df.to_string().replace('\n', '\n\t'))
    
                    datetime_idx_df = derive_idx_df(above_ma_df, numeric_idx=False)
                    close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
    
                    pop_up_datetime_idx_df = datetime_idx_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    pop_up_close_df = close_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    pop_up_close_pct_df = close_pct_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    pop_up_volume_df = volume_df.where(above_ma_df.values).ffill().iloc[[-1]]
                    pop_up_ma_vol_df = ma_vol_df.where(above_ma_df.values).ffill().iloc[[-1]]
    
                    for ticker in ticker_list:
                        display_close = pop_up_close_df.loc[:, ticker].iat[0, 0]
                        display_volume = pop_up_volume_df.loc[:, ticker].iat[0, 0]
                        display_close_pct = round(pop_up_close_pct_df.loc[:, ticker].iat[0, 0], 2)
                        display_ma_vol = pop_up_ma_vol_df.loc[:, ticker].iat[0, 0]
    
                        pop_up_datetime = pop_up_datetime_idx_df.loc[:, ticker].iat[0, 0]
                        pop_up_hour = pd.to_datetime(pop_up_datetime).hour
                        pop_up_minute = pd.to_datetime(pop_up_datetime).minute
                        display_hour = ('0' + str(pop_up_hour)) if pop_up_hour < 10 else pop_up_hour
                        display_minute = ('0' + str(pop_up_minute)) if pop_up_minute < 10 else pop_up_minute
                        display_time_str = f'{display_hour}:{display_minute}'
                        read_time_str = f'{pop_up_hour} {pop_up_minute}' if (pop_up_minute > 0) else f'{pop_up_hour} o clock' 
                        read_ticker_str = " ".join(ticker)
    
                        logger.debug(f'{ticker} ramp up {display_close_pct}% above {ma_val}MA volume, Time: {display_time_str}, {ma_val}MA volume: {display_ma_vol}, Volume: {display_volume}, Close: ${display_close}')
                        print(f'{ticker} ramp up {display_close_pct}% above {ma_val}MA volume, Time: {display_time_str}, {ma_val}MA volume: {display_ma_vol}, Volume: {display_volume}, Close: ${display_close}')
                        text_to_speech_engine.speak(f'{read_ticker_str} ramp up {display_close_pct} percent above {ma_val} M A volume at {read_time_str}')

