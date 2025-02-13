from pandas import DataFrame
from datetime import datetime

from typing import Dict, Optional, Any

from ctax.accounting.finance import download_historic_prices
from ctax.transaction import create_inner_txs
from ctax.utils import convert_to_datetime


def _split_symbol_column(df: DataFrame) -> DataFrame:
    """ """
    df[["asset_1", "asset_2"]] = df["Symbol"].str.split("-", expand=True,)

    return df.drop(columns=["Symbol"])


def _create_txs_history(df: DataFrame, rates: Optional[DataFrame] = None) -> DataFrame:
    """ """

    final_txs = [] # list of dicts

    for _, swap in df.iterrows():
        split_txs = create_inner_txs(swap, rates=rates)
        final_txs.extend(split_txs)

    return DataFrame(final_txs)


def _get_usdt_eur_rates(begin: str = '01-01-2000') -> DataFrame:

    conversion_rates = \
        download_historic_prices(
            symbol="USDT-EUR",
            start=begin,
            end=datetime.now()
        )
    return conversion_rates


def preprocess_kucoin(
    df: DataFrame,
    config: Dict[str,Any]
    ):
    """ """

    first_date = df["Filled Time(UTC+02:00)"].min().split(" ")[0]
    rates = _get_usdt_eur_rates(begin=first_date)

    return df. \
        pipe(_split_symbol_column). \
        rename(columns=config["rename_dict"]). \
        assign(tx_type=lambda x: x["tx_type"].str.lower()). \
        assign(timestamp= convert_to_datetime). \
        pipe(_create_txs_history, rates)
