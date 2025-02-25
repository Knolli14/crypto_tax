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
    that the data is exported from the exchanges when using raw option.

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
    if raw and not cex:
        raise ValueError("You need to specify the exchange"
                         "when loading raw data")
    elif raw and cex:
        load_kwargs = pre_config[cex]

    else:
        load_kwargs = {}


    # Path handling
    file_path = paths.ROOT / paths.DATA_DIR / directory / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"No file found at {file_path}")


    # Routing to correct loader
    match file_path.suffix:

        case ".csv":
            history = _load_csv(file_path, **load_kwargs)

        case ".parquet":
            history = pd.read_parquet(file_path, filters=filters,
                                      engine="pyarrow")
        case _:
            raise ValueError("File needs to be either csv or parquet")


    print(f"...loaded {history.shape[0]} entries")
    return history


def _load_csv(file_path: str | Path, **load_kwargs: dict
              ) -> pd.DataFrame:
    """ Helper Method for loading data from csv files. Responsible for
    Error handling.

    :param **load_kwargs: dictionary with keyword arguments
    for the pd.read_csv method
    """

    try:
        history = pd.read_csv(file_path, **load_kwargs)

    except ParserError:
        print(f"Error parsing file. Check if 'cex' is correctly chosen.")
        history = pd.DataFrame()

    return history


if __name__ == "__main__":

    pass
