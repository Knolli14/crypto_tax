from pathlib import Path
import pandas as pd
from pandas.errors import ParserError
from ctax.config.config import Config
from ctax.config import paths

config = Config.load()
pre_config = config["preprocess"]


def load_history(
    file_name: str | Path,
    *,
    directory: str = "",
    raw: bool = False,
    cex: str = None,
    filters: list[tuple] | list[list[tuple]] = None
) -> pd.DataFrame:
    """Can load either csv or parquet files into a dataframe. It expects
    that the data is exported from the exchanges.

    :param file_name: name of the file to load
    :param directory: subdirectory of datafolder
    :param raw: set True if loading the raw csv exported from the exchange
    :param cex: if raw set to true you need to specify the exchange
    :param filters: filters to apply when loading parquet files
    :raises ValueError: if raw is True and cex is not specified
    :raises ValueError: if the file is not csv or parquet
    :raises FileNotFoundError: if the file is not found
    :raises ParserError: if the csv file cannot be parsed. Probably due to wrong header
    """
    print(f"\nLoading {file_name}...")

    # Checking if cex is specified for a raw import and then loading
    # cex specific config
    if raw:
        if not cex:
            raise ValueError(
                "You need to specify the exchange when loading raw data")
        load_kwargs = pre_config[cex]
    else:
        load_kwargs = {}

    # Path handling
    file_path = paths.ROOT / paths.DATA_DIR / directory / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"No file found at {file_path}")

    # Routing to correct format
    # CSV
    if file_path.suffix == ".csv":
        try:
            return pd.read_csv(file_path, **load_kwargs)

        except ParserError:
            print(f"Error parsing file. Check if 'cex' is correctly chosen.")
            return pd.DataFrame()

    # Parquet
    elif file_path.suffix == ".parquet":

        return pd.read_parquet(file_path,
                               filters=filters,
                               engine="pyarrow")
    else:
        raise ValueError("File needs to be either csv or parquet")


if __name__ == "__main__":

    raw_history_bp = load_history("bitpanda_all.csv", raw=True, cex="bitpanda")
    print(raw_history_bp.head())

    raw_parquet = load_history("bitpanda_all.parquet", directory="test")
    print(raw_parquet.head())
