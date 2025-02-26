import pandas as pd
from typing import Any


def to_datetime(col: pd.Series) -> pd.Series:
    """Converts a column to datetime and removes the timezone information."""

    col = pd.to_datetime(col, utc=True).dt.tz_localize(None)
    return col


def list_arguments(arg: Any) -> list[Any]:
    """Converts a single argument to a list if it is not already a list."""
    return arg if isinstance(arg, list) else [arg]
