import numpy as np

from pandas import DataFrame, Series, merge_asof, to_datetime

from ctax.utils import convert_to_datetime
from ctax.accounting import finance as fin
from ctax.config import load_config
from ctax.paths import CONFIG_PATH

config = load_config(CONFIG_PATH)

base_fiat = config["base_fiat"]
quantity_columns = config["bitpanda"]["quantity_columns"]
rename_dict = config["bitpanda"]["rename_dict"]

print(base_fiat, quantity_columns, rename_dict)

#config = get_config("bitpanda")
#rename_labels = config["rename_dict"]
#quantity_columns = config["quantity_columns"]

# main function
def preprocess_bitpanda(
    df: DataFrame,
    base_fiat: str = "EUR",
    ) -> DataFrame:
    """ """
    print("\nPreprocessing Bitpanda data...")


    return df. \
        pipe(clean_quantity_columns, columns=quantity_columns). \
        rename(columns=rename_dict). \
        assign(timestamp=convert_to_datetime). \
        pipe(convert_forex_columns, base_fiat)


# helper functions

def clean_quantity_columns(
    df: DataFrame,
    columns: list[str]
    ) -> DataFrame:
    """ """
    print("\nCleaning quantity columns...")

    for col in columns:
        df[col] = df[col].fillna(0)

    return df


def convert_forex_columns(
    history: DataFrame,
    base_fiat: str = "EUR",
    ) -> DataFrame:
    """ """
    print(f"\nConverting fiat amounts to {base_fiat}...")

    forex_rates = fin.prepare_rates(history, base_fiat)

    is_base = history.fiat == base_fiat
    rates_result = [is_base]

    for forex_fiat, rates in forex_rates.items():

        ticker = fin.ticker(base_fiat, forex_fiat)
        df_merged = _merge_with_rates(history, rates, ticker)[["fiat", "rate"]]

        is_forex = df_merged.fiat == forex_fiat
        rates_result.append(df_merged.rate * is_forex)

    history["amount_fiat"] = _calculate_new_amount(history, rates_result)
    history["fiat"] = base_fiat

    return history


def _merge_with_rates(
    df: DataFrame,
    rates: Series,
    ticker: str
    ) -> DataFrame:
    """ """

    rates.index = to_datetime(rates.index, utc=True)

    return \
        merge_asof(df, rates, right_index=True, left_on="timestamp"). \
        rename(columns={ticker: "rate"})


def _calculate_new_amount(
    df: DataFrame,
    rates_list: list[Series]
    ) -> Series:
    """ """

    return np.round(
        np.sum(rates_list, axis=0) * df.amount_fiat,
        decimals=2
    )
