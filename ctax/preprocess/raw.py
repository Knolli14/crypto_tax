from pandas import DataFrame, to_datetime

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

    processed_df = \
        preprocessing_funcs[cex](df). \
        pipe(post_process)

    return processed_df


def post_process(df: DataFrame) -> DataFrame:
    """ """
    return df. \
        assign(timestamp = _to_datetime). \
        pipe(_to_categorical)

def _to_categorical(df: DataFrame) -> DataFrame:
    """
    Helper function that converts object columns to categorical after loading
    history from file.
    """
    obj_cols = df.select_dtypes(include="object").columns
    return df.astype({col: "category" for col in obj_cols})


def _to_datetime(df: DataFrame) -> DataFrame:
    """ """
    return to_datetime(df["timestamp"]).dt.tz_localize(None)
