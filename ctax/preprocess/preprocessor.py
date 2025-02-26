from pathlib import Path
import pandas as pd
from typing import Generator
from ctax.preprocess.cex.bitpanda import process_bitpanda
from ctax.preprocess.cex.kucoin import process_kucoin
from ctax.dataio.loading import load_history
from ctax.dataio.saving import save_history
from ctax.utils import list_arguments, check_same_lengths


class Preprocessor:
    """ """

    # --- Class Attributes ---
    _cex_catalogue = {"kucoin": process_kucoin,
                      "bitpanda": process_bitpanda}

    _cex_workers = {}

    # ---  Initialization ---
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
            self.cex_func = self._cex_catalogue[cex]
            self.raw = None
            self.processed = []
            self.source = None
            self._initialized = True


    # --- Instance Creation ---
    @classmethod
    def from_file(cls, file_name: str | Path, cex: str) -> "Preprocessor":
        """ Intended method to create a Preprocessor instance."""

        pp = cls(cex)
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


    # --- Main Instance Methods ---
    def preprocess_history(self) -> "Preprocessor":
        """ """
        if self.raw is None:
            print("Please load raw history first")
            return self

        processed_history = self.cex_func(self.raw)
        self._add_processed(processed_history)

        return self


    # --- Saving Method ---
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
        self.processed.append(history)

        return None


    # --- Main Class Method ---
    @classmethod
    def preprocess_files(
            cls, file_name: str | Path | list[str | Path],
            cex: str, *,
            output_file: str = None,
            merge: bool = True
        ) -> None:
        """ Preprocesses raw csv files and saves the processed history
        to a parquet. Merges files on default."""

        files = list_arguments(file_name)
        cexs = list_arguments(cex)

        check_same_lengths([files, cexs])

        for file, cex in zip(files, cexs): (
                cls.from_file(file, cex)
                .preprocess_history()
                .save_processed_history(output_file)
        )
        if merge and len(files) > 1:

            histories = cls._fetch_histories("all")
            merged_history = cls._merge_histories(histories)
            save_history(merged_history, "merged.parquet", directory="processed")

        return None


    # --- Helper Methods ---
    @classmethod
    def _fetch_histories(cls, cex: str = "all") -> Generator:
        """ """

        if cex == "all":
            workers = cls._cex_workers.values()
            histories = (h for worker in workers
                         for h in worker.processed)
        else:
            histories = (h for h in cls._cex_workers[cex].processed)

        return histories


    @classmethod
    def _merge_histories(cls, histories: Generator) -> pd.DataFrame:
        """ """
        print("\nMerging files...")

        def _drop_dups(df: pd.DataFrame) -> pd.DataFrame:
            subset = ["tx_id", "tx_type", "amount_asset", "amount_base"]
            num_duplicated = df.duplicated(subset=subset).sum()

            df = df.drop_duplicates(subset=subset)
            print(f"-> Dropped {num_duplicated} duplicates")

            return df

        merged_history =  (
            pd.concat(histories, ignore_index=True)
            .pipe(_drop_dups)
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        print(f"-> New History has {len(merged_history)} entries")

        return merged_history


if __name__ == "__main__":

    pass
