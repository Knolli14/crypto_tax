import pandas as pd
from pandas import DataFrame

from pathlib import Path
from typing import Literal, List, Dict, Any

from ctax.config import CONFIG
from ctax.data import open_csv, open_parquet, save_as_parquet
from ctax.paths import convert_extension
from ctax.utils import print_success


def preprocess_raw_df(
    df: DataFrame,
    cex: Literal["kucoin", "bitpanda"],
    config: Dict[str, Any],
    ) -> DataFrame:
    """ """

    if df is None:
        return pd.DataFrame()

    if cex not in ["kucoin", "bitpanda"]:
        raise ValueError("Unknown exchange. Please use 'kucoin' or 'bitpanda'.")

    print(f"Start Preprocessing {cex} data...")
    return config["preprocessing_func"](df, config)


# Merging

def merge_history(histories: List[DataFrame]) -> DataFrame:
    """ """

    print("\nMerging all histories into one DataFrame...")
    return \
        pd.concat(histories, axis=0, ignore_index=True). \
        sort_values(by="timestamp", ascending=True, ignore_index=True)


def _create_complete_history(output_files: str|Path) -> DataFrame:
    """ """

    histories = [open_parquet(file) for file in output_files]
    return merge_history(histories)


def _convert_to_list(x):
    return x if isinstance(x, list) else [x]


def preprocess_directly(
    file: str|Path,
    cex: str,
    config: Dict[str, Any]
    ) -> None:

    output_file = convert_extension(file, "parquet")
    return \
        open_csv(file, config["csv_import"]). \
        pipe(preprocess_raw_df, cex, config). \
        pipe(save_as_parquet, output_file, new_file=False) # change override behaviour


# Main function

def preprocess_histories(
    csv_file: str|Path|List[str|Path],
    cex: Literal["kucoin", "bitpanda"]|List[Literal["kucoin", "bitpanda"]],
    ) -> None:
    """ """

    csv_files = _convert_to_list(csv_file)
    cexs = _convert_to_list(cex)
    output_files = []

    # in case we have multiple files, we need the same number of cex
    if len(csv_files) != len(cexs):
        raise ValueError("csv_files and cex must have the same length.")


    for file, cex in zip(csv_files, cexs):

        config = CONFIG[cex]

        preprocess_directly(file, cex, config)
        print(f"-> Finished Preprocess of {file}" )

        output_files.append(convert_extension(file, "parquet"))

    print_success( "-> Done converting raw csv files to parquet!")

    # merge all dfs and save as parquet
    if len(csv_files) > 1:
        output_file_complete = "full_history.parquet"

        return \
            _create_complete_history(output_files). \
            pipe(save_as_parquet, output_file_complete, new_file=False)

    return None
