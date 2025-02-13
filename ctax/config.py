from ctax.preprocess.bitpanda import preprocess_bitpanda
from ctax.preprocess.kucoin import preprocess_kucoin

FINAL_COLUMN_LABELS = [
    "timestamp",
    "tx_type",
    "amount_asset",
    "asset",
    "amount_fiat",
    "fiat",
]

CONFIG = {
    "bitpanda": {
        "csv_import" :{
            "usecols":(cols := [
                "Timestamp",
                "Transaction Type",
                "Amount Asset",
                "Asset",
                "Amount Fiat",
                "Fiat",
            ]),
            "header": 6,
            "na_values": ["-"],
        },
        "quantity_columns" : [
            "Amount Asset",
            "Amount Fiat",
        ],
        "rename_dict": dict(zip(cols, FINAL_COLUMN_LABELS)),
        "preprocessing_func": preprocess_bitpanda,
    },
    "kucoin": {
        "csv_import": {
            "usecols": [
                "Filled Time(UTC+02:00)",
                "Side",
                "Symbol",
                "Filled Amount", #amount of asset_1
                "Filled Volume", #amount of asset_2
                "Filled Volume (USDT)", # fiat value
            ],
        },
        "rename_dict": {
            "Side": "tx_type",
            "Filled Amount": "amount_asset_1",
            "Filled Volume": "amount_asset_2",
            "Filled Volume (USDT)": "amount_usdt",
            "Filled Time(UTC+02:00)": "timestamp",
        },
        "preprocessing_func": preprocess_kucoin,
    }
}
