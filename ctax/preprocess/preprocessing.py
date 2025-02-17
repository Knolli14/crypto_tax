import pandas as pd
from pandas import DataFrame

from pathlib import Path
from typing import Literal

from ctax.data import load_history, save_history
from ctax.paths import convert_extension
from ctax.utils import print_success

from ctax.preprocess.kucoin import preprocess_kucoin
from ctax.preprocess.bitpanda import preprocess_bitpanda


def preprocess_raw_df(
    df: DataFrame,
    cex: Literal["kucoin", "bitpanda"],
    ) -> DataFrame:
    """ """
    print(f"Start Preprocessing {cex} data...")

    preprocess_funcs = {
        "kucoin": preprocess_kucoin,
        "bitpanda": preprocess_bitpanda
    }

    if df is None:
        return pd.DataFrame()

    if cex not in ["kucoin", "bitpanda"]:
        raise ValueError("Unknown exchange. Please use 'kucoin' or 'bitpanda'.")

    preprocess_func  = preprocess_funcs[cex]

    return preprocess_func(df)


def preprocess_directly(
    file_name: str | Path,
    cex: str,
    ) -> None:

    output_file = convert_extension(file_name, "parquet")
    df = load_history(
        file_name,
        directory="csv",
        raw=True,
        cex=cex
    )
    print(f"-> Loaded {file_name}")

    return df. \
        pipe(preprocess_raw_df, cex). \
        pipe(save_history, output_file, new_file=False) # change override behaviour


# Merging

def merge_history(histories: list[DataFrame]) -> DataFrame:
    """ """
    print("\nMerging all histories into one DataFrame...")

    return \
        pd.concat(histories, axis=0, ignore_index=True). \
        sort_values(by="timestamp", ascending=True, ignore_index=True)


def _create_complete_history(output_files: str | Path) -> DataFrame:
    """ """

    histories = [load_history(file, directory="parquet") for file in output_files]
    return merge_history(histories)


def _convert_to_list(x):
    return x if isinstance(x, list) else [x]




# Main function

def preprocess_histories(
    csv_file: str | Path | list[str|Path],
    cex: Literal["kucoin", "bitpanda"] | list[Literal["kucoin", "bitpanda"]],
    ) -> None:
    """ """

    csv_files = _convert_to_list(csv_file)
    cexs = _convert_to_list(cex)
    output_files = []

    # in case we have multiple files, we need the same number of cex
    if len(csv_files) != len(cexs):
        raise ValueError("csv_files and cex must have the same length.")


    for file, cex in zip(csv_files, cexs):


        preprocess_directly(file, cex)
        print(f"-> Finished Preprocess of {file}" )

        output_files.append(convert_extension(file, "parquet"))

    print_success( "-> Done converting raw csv files to parquet!")

    # merge all dfs and save as parquet
    if len(csv_files) > 1:
        output_file_complete = "full_history.parquet"

        return \
            _create_complete_history(output_files). \
            pipe(save_history, output_file_complete, new_file=False)

    return None
