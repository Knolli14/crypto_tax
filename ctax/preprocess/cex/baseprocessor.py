import pandas as pd
from abc import ABC, abstractmethod
from ctax.config.config import Config
from ctax.utils import to_datetime


class BaseProcessor(ABC):
    """"""

    config = Config.load()
    final_column_labels = config["preprocess"]["final_columns"]

    date_column_label = final_column_labels["timestamp"]

    cat_column_labels = [
        final_column_labels["tx_type"],
        final_column_labels["asset"],
        final_column_labels["base_asset"],
    ]
    quantity_column_labels = [
        final_column_labels["amount_asset"],
        final_column_labels["amount_base"],
    ]

    @classmethod
    @abstractmethod
    def preprocess(cls, history: pd.DataFrame) -> pd.DataFrame:
        """Implements the cex-specific preprocess steps. Comes as the first
        step in the processing pipeline."""
        pass


    @property
    @abstractmethod
    def rename_dict(cls) -> dict:
        """Dictionary containing the mapping of the old column labels to the
        final column labels."""
        pass

    #@property
    #@abstractmethod
    #def load_keywords(cls) -> dict:
    #    """Dictionary containing the keywords that are used when loading the
    #    raw transaction history with pd.read_csv."""
    #    pass


    @classmethod
    def process(cls, history: pd.DataFrame) -> pd.DataFrame:
        """ """

        return (
            history
            .pipe(cls.preprocess)
            .pipe(cls.postprocess)
        )


    @classmethod
    def postprocess(cls, history: pd.DataFrame) -> pd.DataFrame:
        """Comes after the cexspecific preprocess. It converts the column with
        the transaction dates to datetime and converts columns with categorical
        data to the categorical type.
        """
        print("-> Postprocessing data...")

        history = (
            history
            .pipe(cls._set_final_column_labels)
            .pipe(cls._reorder_columns)
            .pipe(cls._convert_dypes)
        )
        print("...done")
        return history

    @classmethod
    def _reorder_columns(cls, history: pd.DataFrame) -> pd.DataFrame:
        """ Reorders the columns in the DataFrame. """
        print("  -> Reordering columns...")

        desired_order = [
            cls.final_column_labels["timestamp"],
            cls.final_column_labels["tx_type"],
            cls.final_column_labels["amount_asset"],
            cls.final_column_labels["asset"],
            cls.final_column_labels["amount_base"],
            cls.final_column_labels["base_asset"],
        ]
        return history[desired_order]

    @classmethod
    def _set_final_column_labels(cls, history: pd.DataFrame) -> dict:
        """
        """
        print("  -> Renaming columns...")
        return history.rename(columns=cls().rename_dict)


    @classmethod
    def _convert_dypes(cls, history: pd.DataFrame) -> pd.DataFrame:
        """
        """
        print("  -> Converting columns to different dtypes...")

        history[cls.date_column_label] = (
            to_datetime(history[cls.date_column_label])
        )
        history = history.astype(
            {col: "category" for col in cls.cat_column_labels}
        )
        history = history.astype(
            {col: "float64" for col in cls.quantity_column_labels}
        )
        return history
