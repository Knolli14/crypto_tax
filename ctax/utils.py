import pandas as pd


def to_datetime(col: pd.Series) -> pd.Series:
    """Converts a column to datetime and removes the timezone information."""

    col = pd.to_datetime(col, utc=True).dt.tz_localize(None)
    return col
