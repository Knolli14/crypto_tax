import pandas as pd
from typing import Any


def to_datetime(col: pd.Series) -> pd.Series:
    """Converts a column to datetime and removes the timezone information."""

    col = pd.to_datetime(col, utc=True).dt.tz_localize(None)
    return col


def list_arguments(arg: Any) -> list[Any]:
    """Converts a single argument to a list if it is not already a list."""
    return arg if isinstance(arg, list) else [arg]


def check_same_lengths(args: list) -> bool:
    """Checks if all arguments have the same length."""
    same_lengths = len(set(map(len, args))) == 1
    if not same_lengths:
        raise ValueError("Arguments must have the same length.")
    return True
