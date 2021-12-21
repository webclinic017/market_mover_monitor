from pandas.core.frame import DataFrame
import pandas as pd
import numpy as np

from constant.indicator.runtime_indicator import RuntimeIndicator

idx = pd.IndexSlice

def derive_idx_df(src_df: DataFrame) -> DataFrame:
    idx_np = src_df.reset_index(drop=True).reset_index().loc[:, idx['index', :]].values
    return pd.DataFrame(np.repeat(idx_np, len(src_df.columns), axis=1), 
                        columns=src_df.columns).rename(columns={src_df.columns.get_level_values(1).values[0]: RuntimeIndicator.INDEX})