import pandas as pd
from pandas.errors import ParserError

from pathlib import Path
from typing import Dict, List, Any

from ctax.paths import DATA_DIR, create_file_path



def open_csv(
    file_name: str|Path,
    config: Dict[str, Any] = None,
    ) -> pd.DataFrame:
    """

    """

    file_path = DATA_DIR / "csv" / file_name

    try:
        print(f"\nOpening {file_path}")
        return pd.read_csv(file_path, **config or {})

    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None

    except ParserError:
        print(f"Error parsing file. Probably you need to adjust the header.")
        return None


def save_as_parquet(
    df: pd.DataFrame,
    output_file: str|Path,
    new_file: bool = True,
    engine: str = "pyarrow"
    ) -> None:
    """

    """

    file_path = \
        create_file_path(output_file, "parquet") if new_file else \
        DATA_DIR / "parquet" / output_file

    df.to_parquet(file_path, index=False)

    print(f"...saved to {file_path}")
    return None


def open_parquet(
    file_name: str|Path,
    filters: List[tuple]|List[List[tuple]]=None,
    ) -> pd.DataFrame:
    """

    """

    file_path = DATA_DIR / "parquet" / file_name

    try:
        return \
            pd.read_parquet(
                file_path,
                filters=filters,
                engine="pyarrow"
            ). \
            assign(timestamp = lambda df: \
                pd.to_datetime(df["timestamp"]).\
                dt.tz_localize(None)
            )

    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
