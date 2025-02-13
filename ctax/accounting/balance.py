from pandas import DataFrame
from pathlib import Path
from typing import List

from ctax.data import open_parquet

# main function

def create_asset_balance(file: str|Path, asset: str) -> DataFrame:
    """ """

    filters = [
        ("tx_type", "in", (rows := ["buy", "sell", "reward"])),
        ("asset", "==", asset)
    ]

    df_total = \
        open_parquet(file, filters=filters). \
        groupby("tx_type")[["amount_asset", "amount_fiat"]].sum(). \
        pipe(_create_empty_rows,rows). \
        pipe(_create_total_row). \
        assign(avg=lambda df: df.amount_fiat / df.amount_asset)

    return df_total


# helper functions

def _create_total_row(df: DataFrame) -> DataFrame:
    """ """

    df.loc["total"] = {
        "amount_asset": _calculate_total(df, "amount_asset"),
        "amount_fiat": -_calculate_total(df, "amount_fiat")
    }
    return df

def _create_empty_rows(df: DataFrame, rows: List[str]) -> DataFrame:
    """ """

    for row in rows:
        if row not in df.index:
            df.loc[row] = [0, 0]

    return df

def _calculate_total(df: DataFrame, column: str) -> float:
    """ """

    buy = df.loc["buy", column]
    sell = df.loc["sell", column]
    reward = df.loc["reward", column]

    return (buy + reward) - sell
