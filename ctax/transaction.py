import numpy as np
from pandas import Series, DataFrame

from dataclasses import dataclass, asdict

from ctax.accounting.finance import get_conversion_rate


@dataclass #-> in case we need methods
class Transaction:

    timestamp: np.datetime64
    tx_type: str
    amount_asset: np.float64
    asset: str
    amount_fiat: np.float64
    fiat: str

    def to_dict(self):
        return asdict(self)


def create_inner_txs(
    swap: Series,
    rates: DataFrame=None
    ) -> list[Transaction]:

    was_sold = swap["tx_type"]=="sell"
    timestamp = swap["timestamp"]
    eur_value = np.round(swap["amount_usdt"] *
                get_conversion_rate(timestamp.date(), conversion_rates=rates), 2)

    inner_1 = Transaction(
        timestamp,
        "sell",
        swap["amount_asset_1"] if was_sold else swap["amount_asset_2"],
        swap["asset_1"] if was_sold else swap["asset_2"],
        eur_value,
        "EUR",

    )

    inner_2 = Transaction(
        timestamp,
        "buy",
        swap["amount_asset_2"] if was_sold else swap["amount_asset_1"],
        swap["asset_2"] if was_sold else swap["asset_1"],
        eur_value,
        "EUR",

    )

    return [inner_1, inner_2]
