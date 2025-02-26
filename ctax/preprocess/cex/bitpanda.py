
import pandas as pd

from ctax.preprocess.cex.baseprocessor import BaseProcessor


# Class
class BitpandaProcessor(BaseProcessor):
    """Preprocesses the Bitpanda transaction history that was downloaded.
    from the Bitpanda webapp through the export function. Data comes in a csv"""

    @classmethod
    def preprocess(cls, history: pd.DataFrame) -> pd.DataFrame:
        """Only replaces the Nan values in the quantity columns with 0.
        Transactions are already transformed to fiat Transactions by
        bitpanda."""

        print("\n-> Preprocessing Bitpanda data...")
        return cls._clean_quantity_columns(history)


    @property
    def rename_dict(cls) -> dict:
        """"""
        return {
            "Timestamp": cls.final_column_labels["timestamp"],
            "Transaction Type": cls.final_column_labels["tx_type"],
            "Amount Asset": cls.final_column_labels["amount_asset"],
            "Asset": cls.final_column_labels["asset"],
            "Amount Fiat": cls.final_column_labels["amount_base"],
            "Fiat": cls.final_column_labels["base_asset"],
            "Transaction ID": cls.final_column_labels["tx_id"],
        }

    #@property
    #def load_keywords(cls) -> dict:
    #    return cls.config["preprocess"]["bitpanda"]


    @staticmethod
    def _clean_quantity_columns(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """ Replaces NaN values in the columns for the amount of assets. They
        started as '-' in the csv file and were converted to NaN during import.
        """
        print("  -> Cleaning quantity columns...")

        quantitiy_columns = ["Amount Asset", "Amount Fiat"]

        return df.fillna({col: 0. for col in quantitiy_columns})


def process_bitpanda(
    df: pd.DataFrame
) -> pd.DataFrame:
    """Function that processes the raw Bitpanda transaction history. Under the
    hood it uses the BitpandaProcessor class."""
    return BitpandaProcessor.process(df)
