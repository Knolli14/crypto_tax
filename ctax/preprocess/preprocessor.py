from pathlib import Path
import pandas as pd
from ctax.preprocess.cex import bitpanda as bp
from ctax.preprocess.cex import kucoin as kc
from ctax.dataio.loading import load_history
from ctax.dataio.saving import save_history
from ctax.utils import list_arguments


class Preprocessor:
    """ """

    _cex_catalogue = {"kucoin": kc.KucoinProcessor,
                      "bitpanda": bp.BitpandaProcessor}

    _cex_workers = {}

    def __new__(cls, cex: str):
        """ """
        if cex not in cls._cex_catalogue:
            raise ValueError(f"{cex} is not supported"
                             f"Choose from {cls._cex_catalogue.keys()}")

        if cex not in cls._cex_workers:
            instance =  super().__new__(cls)
            cls._cex_workers[cex] = instance

        return cls._cex_workers[cex]


    def __init__(self, cex: str):
        if not hasattr(self, "_initialized"):
            self.cex = cex
            self.cex_worker = self._cex_catalogue[cex]
            self.raw = None
            self.processed = []
            self.source = None
            self._initialized = True

    # --- Instance Creation ---
    @classmethod
    def from_file(cls, file_name: str | Path, cex: str) -> "Preprocessor":
        """ Intended method to create a Preprocessor instance."""

        pp = cls(cex)
        print(Preprocessor.__dict__)
        print(pp.__dict__)
        raw_history = load_history(file_name, directory="raw", cex=cex,
                                   raw=True)

        pp._set_raw_history(raw_history)
        pp.source = file_name

        return pp


    @classmethod
    def _from_dataframe(cls, df: pd.DataFrame, cex: str) -> "Preprocessor":
        """ """
        pp = cls(cex)
        pp._set_raw_history(df)
        return pp


    # --- Methods ---
    def preprocess_history(self) -> "Preprocessor":
        """ """
        if self.raw is None:
            print("Please load raw history first")
            return self

        processed_history = self.cex_worker.process(self.raw)
        self._add_processed(processed_history)

        return self


    def save_processed_history(self, file_name: str | Path = None) ->  None:
        """ """
        output_file = (file_name if file_name else
                     Path(self.source).stem + ".parquet")
                    # TODO: replace with config setting

        save_history(self.processed[-1],
                     output_file or "processed.parquet",
                     directory="processed")

        return None

    # --- Setters ---
    def _set_raw_history(self, history: pd.DataFrame) -> "Preprocessor":
        """ Saves the raw history to the instance. """
        self.raw = history
        return self


    def _add_processed(self, history: pd.DataFrame) -> None:
        """"""
        print(self.processed)
        self.processed.append(history)

        return None


    # --- Getters ---
    @classmethod
    def get_worker(cls, cex: str) -> "Preprocessor":
        """ """
        return cls._cex_workers[cex]


    # --- Class Methods ---
    @classmethod
    def preprocess_files(
            cls, file_name: str | Path | list[str | Path],
            cex: str, *,
            output_file: str = None,
            save: bool = True
        ) -> None:
        """ Preprocesses raw csv files and saves the processed history
        to a parquet"""

        files = list_arguments(file_name)
        cexs = list_arguments(cex)

        if len(files) != len(cexs):
            raise ValueError("Number of files and cexs must match")

        for file_name, cex in zip(files, cexs):

            pp = cls.from_file(file_name, cex).preprocess_history()
            pp.save_processed_history(output_file) if save else None

        return None


    @classmethod
    def merge_files(cls):
        pass


    @classmethod
    def _fetch_histories(cls, cex: str = "all") -> list[pd.DataFrame]:
        """ """

        if cex == "all":
            workers = cls._cex_workers.values()
            histories = (h for worker in workers
                         for h in worker.processed)
        else:
            histories = (h for h in cls._cex_workers[cex].processed)

        return histories

    @classmethod
    def _merge_histories(cls, histories: list[pd.DataFrame]) -> pd.DataFrame:
        """ """

        merged_history =  (
            pd.concat(histories, ignore_index=True)
            .drop_duplicates(subset=["tx_id", "amount_asset", "amount_base"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        return merged_history

# --- File Preprocessing Functions ---
def preprocess_bitpanda_file(file_name: str | Path) -> None:
    """ ready to use function to preprocess a bitpanda file
    and save it back to a parquet file"""
    Preprocessor.preprocess_file(file_name, "bitpanda")

def preprocess_kucoin_file(file_name: str | Path) -> None:
    """ ready to use function to preprocess a kucoin file
    and save it back to a parquet file"""
    Preprocessor.preprocess_file(file_name, "kucoin")


if __name__ == "__main__":

    pass
