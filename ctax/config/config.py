import yaml
from pathlib import Path
from ctax.paths import CONFIG_PATH

def create_config(file_path: str | Path) -> None:
    """ """

    config = {
        "base_fiat": "EUR",
        "bitpanda": {
            "csv_import": {
                "usecols": [
                    "Timestamp",
                    "Transaction Type",
                    "Amount Asset",
                    "Asset",
                    "Amount Fiat",
                    "Fiat",
                ],
                "header": 6,
                "na_values": ["-"],
            },
            "quantity_columns": ["Amount Asset", "Amount Fiat"],
            "rename_dict": {
                "Timestamp": "timestamp",
                "Transaction Type": "tx_type",
                "Amount Asset": "amount_asset",
                "Asset": "asset",
                "Amount Fiat": "amount_fiat",
                "Fiat": "fiat",
            },
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
                "Filled Time(UTC+02:00)": "timestamp",
                "Side": "tx_type",
                "Filled Amount": "amount_asset_1",
                "Filled Volume": "amount_asset_2",
                "Filled Volume (USDT)": "amount_usdt",
            },
        }
    }
    with open(file_path, "w") as file:
        yaml.dump(config, file)

    return None

def load_config(file_path: str) -> dict:
    """ """

    with open(file_path, "r") as file:
        config = yaml.safe_load(file)

    return config


if __name__ == "__main__":
    create_config(CONFIG_PATH)
