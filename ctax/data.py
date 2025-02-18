from pandas import DataFrame, read_csv, read_parquet
from pandas.errors import ParserError

from pathlib import Path

from ctax.config.config import load_config
config = load_config()

from ctax.paths import DATA_DIR, create_file_path

from ctax.preprocess.load import process_loaded_history



def load_history(
    file_name: str | Path,
    *,
    directory: str = "",
    raw: bool = False,
    cex: str = None,
    filters: list[tuple] | list[list[tuple]] = None
) -> DataFrame:
    """
    Can load either csv or parquet files into a dataframe. It expects
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
            raise ValueError("You need to specify the exchange when loading raw data")

        load_kwargs = config[cex]["csv_import"]


    # Path handling
    file_path = DATA_DIR / directory / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"No file found at {file_path}")

    # Routing to correct format
    if file_path.suffix == ".csv":
        try:
            return  read_csv(file_path, **load_kwargs)

        except ParserError:
            print(f"Error parsing file. Probably you need to adjust the header.")
            return DataFrame()

    elif file_path.suffix == ".parquet":

        return \
            read_parquet(
                file_path,
                filters=filters,
                engine="pyarrow"
            ). \
            pipe(process_loaded_history)

    else:
        raise ValueError("File needs to be either csv or parquet")



def save_history(
    df: DataFrame,
    file_name: str | Path,
    *,
    directory: str = "",
    new_file: bool = False,
) -> None:
    """
    Saves the dataframe to a parquet file in the specified directory.

    :param df: dataframe containing tx history to save
    :param file_name: name of the file to save
    :param directory: subdirectory of datafolder
    :param new_file: if False the file will be overwritten
    """
    print(f"\nSaving {file_name}...")

    dir_path = DATA_DIR / directory
    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = \
        create_file_path(file_name, directory) if new_file else \
        dir_path / file_name
    try:
        df.to_parquet(file_path , index=False)
    except Exception as e:
        print("Something went wrong while saving", e)

    print(f"...saved to {file_path}")
    return None
