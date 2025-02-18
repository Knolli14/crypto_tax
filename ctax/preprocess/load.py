from pandas import DataFrame, to_datetime

def process_loaded_history(df: DataFrame) -> DataFrame:
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
