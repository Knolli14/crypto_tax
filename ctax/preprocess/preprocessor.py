from pathlib import Path
import pandas as pd
from ctax.preprocess.cex import bitpanda as bp
from ctax.preprocess.cex import kucoin as kc
from ctax.dataio.loading import load_history
from ctax.dataio.saving import save_history


class Preprocessor:
    """ """

    cex_workers = {"kucoin": kc.KucoinProcessor,
                   "bitpanda": bp.BitpandaProcessor}


    def __init__(self, cex: str):
        if cex not in self.cex_workers:
            raise ValueError(f"{cex} is not supported"
                             f"Choose from {self.cex_workers.keys()}")
        self.cex = cex

        self.cex_worker = self.cex_workers[cex]
        self.histories = {"raw": None, "processed": None}
        self.source = None


    def load_raw_history(self, file_name: str | Path) -> "Preprocessor":
        """ """

        raw_history = load_history(file_name, directory="raw", cex=self.cex,
                                   raw=True)

        self._add_raw_history(raw_history)
        self.source = file_name

        return self


    def save_processed_history(self, file_name: str | Path = None) ->  None:
        """ """
        output_file = (file_name if file_name else
                     Path(self.source).stem + ".parquet")

        save_history(self.histories["processed"],
                     output_file,
                     directory="processed")

        return None


    def _add_raw_history(self, history: pd.DataFrame):
        self.histories["raw"] = history


    def _add_processed_history(self, history: pd.DataFrame):
        self.histories["processed"] = history


    def preprocess(self) -> "Preprocessor":
        """ """

        processed_history = self.cex_worker.process(self.histories["raw"])
        self._add_processed_history(processed_history)

        return self


    @classmethod
    def preprocess_file(
        cls,
        file_name: str | Path,
        cex: str,
        *,
        output_file: str = None
    ) -> None:
        """ """
        return (cls(cex)
                .load_raw_history(file_name)
                .preprocess()
                .save_processed_history(output_file))



def preprocess_bitpanda_file(file_name: str | Path) -> None:
    Preprocessor.preprocess_file(file_name, "bitpanda")

def preprocess_kucoin_file(file_name: str | Path) -> None:
    Preprocessor.preprocess_file(file_name, "kucoin")


if __name__ == "__main__":

    pass
