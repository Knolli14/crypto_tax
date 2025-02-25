import yaml
from pathlib import Path
from ctax.config.paths import CONFIG_FILE


class Config:
    config_dir = Path(__file__).resolve().parent
    file_path = config_dir / CONFIG_FILE

    # Making it a singleton Class to have only one shared Instance over all
    # modules
    _isinstance = None

    # TODO: dig deeper into topic
    def __new__(cls, *args, **kwargs):
        if cls._isinstance is None:
            cls._isinstance = super(Config, cls).__new__(cls)
        return cls._isinstance

    def __init__(self, *, config=None):
        if not hasattr(self, "config"):
            if config is None:
                raise ValueError("config_name must be provided")
            self.config = config

    # Dunder methods
    # TODO: improve and implement a get sections etc
    def __getitem__(self, key):
        try:
            return self.config[key]
        except KeyError:
            print("Not a valid key -> Returning None")
            return None

    def __repr__(self):
        return str(self.config)

    # class methods

    @classmethod
    def load(cls):

        if not cls.file_path.exists():
            raise FileNotFoundError(
                f"No config file found at {cls.file_path}."
                "Use Config.create_config() to create one"
            )

        with open(cls.file_path, "r") as file:
            config = yaml.safe_load(file)
            return cls(config=config)


    @classmethod
    def create_config(cls) -> None:
        """ """
        config = {
            ## Save Settings
            "save_config": {
                "file_name": "history",
                "file_extension": ".parquet",
                "overwrite": False,
            },
            ## CEX specific settings for import and preprocess
            "preprocess": {
                "final_columns": {
                    "timestamp": "timestamp",
                    "tx_type": "tx_type",
                    "asset": "asset",
                    "amount_asset": "amount_asset",
                    "base_asset": "base_asset",
                    "amount_base": "amount_base",
                },
                # --- Bitpanda ---
                "bitpanda": {
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
                # --- Kucoin ---
                "kucoin": {
                    "usecols": [
                        "Filled Time(UTC+02:00)",
                        "Side",
                        "Symbol",
                        "Filled Amount",  # amount of asset_1
                        "Filled Volume",  # amount of asset_2
                        "Filled Volume (USDT)",  # fiat value
                    ],
                }
            }
        }
        with open(cls.file_path, "w") as file:
            yaml.dump(config, file)

        print("Config file created")
        return None


if __name__ == "__main__":
    Config.create_config()
