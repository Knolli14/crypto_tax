from pandas import DataFrame

from typing import Literal

from ctax.preprocess.kucoin import preprocess_kucoin
from ctax.preprocess.bitpanda import preprocess_bitpanda


preprocessing_funcs = {
    "kucoin": preprocess_kucoin,
    "bitpanda": preprocess_bitpanda
}


def preprocess_raw_history(
    df: DataFrame,
    cex: Literal["kucoin", "bitpanda"],
    ) -> DataFrame:
    """ """

    processed_df = preprocessing_funcs[cex](df)

    #TODO: look back into it
    # Im converting it here since i got an incompatible timeformat error during merge with conversion rates
    processed_df["timestamp"] = processed_df["timestamp"].dt.tz_localize(None)

    return processed_df
