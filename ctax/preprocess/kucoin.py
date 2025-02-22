from pandas import DataFrame
from datetime import datetime
from typing import Optional

from ctax.accounting.finance import download_historic_prices
from ctax.transaction import create_inner_txs
from ctax.utils import convert_to_datetime
from ctax.config.config import load_config

config = load_config()
rename_dict = config["kucoin"]["rename_dict"]


# main function
def preprocess_kucoin(
    df: DataFrame,
    ):
    """ """
    print(f"Start Preprocessing Kucoin data...")

    first_date = df["Filled Time(UTC+02:00)"].min().split(" ")[0]
    rates = _get_usdt_eur_rates(begin=first_date)

    return df. \
        pipe(_split_symbol_column). \
        rename(columns=rename_dict). \
        assign(tx_type=_lower,
               timestamp= convert_to_datetime). \
        pipe(_create_txs_history, rates)


# helper functions

def _lower(df: DataFrame) :
    """ """
    return df["tx_type"].str.lower()

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
