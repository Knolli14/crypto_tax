import pandas as pd
from pathlib import Path
from ctax.config import paths
from ctax.config.config import Config

config = Config.load()
save_options = config["save_config"]

output_file = save_options["file_name"]
overwrite = save_options["overwrite"]


def save_history(
    df: pd.DataFrame,
    file_name: str | Path = None,
    *,
    directory: str = "",
) -> None:
    """
    Saves the dataframe to a parquet file in the specified directory if given.
    It is not intended to be used for saving raw data as parquets. Raw data
    should be preprocessed and saved as parquet afterwards.

    :param df: dataframe containing tx history to save
    :param file_name: name of the file to save
    :param directory: subdirectory of datafolder
    :param new_file: if False the file will be overwritten
    """

    file_name = file_name if file_name else output_file
    print(f"\nSaving {file_name}...")

    dir_path = paths.ROOT / paths.DATA_DIR / directory
    dir_path.mkdir(parents=True, exist_ok=True)

    if overwrite:
        file_path = dir_path / file_name

    else:
        file_path = paths.create_unique_file_path(
            file_name=file_name,
            directory=directory
        )

    try:
        df.to_parquet(file_path, index=False)
        print(f"...saved to {file_path}")

    except Exception as e:
        print("Something went wrong while saving", e)

    return None


if __name__ == "__main__":

    pass
