from ctax.preprocess.preprocessor import Preprocessor
from ctax.dataio.loading import load_history
from ctax.dataio.saving import save_history
from ctax.utils import list_arguments
from pathlib import Path

# --- File Preprocessing Functions ---
def preprocess_bitpanda_files(file_names: str | Path | list) -> None:
    """ ready to use function to preprocess bitpanda files
    and save it back to a parquet file. """
    for file in list_arguments(file_names):
        Preprocessor.preprocess_files(file, "bitpanda")
    return None


def preprocess_kucoin_files(file_names: str | Path) -> None:
    """ ready to use function to preprocess kucoin files
    and save it back to a parquet file"""
    for file in list_arguments(file_names):
        Preprocessor.preprocess_files(file, "kucoin")
    return None


def merge_files(
        files: list[str | Path],
        output_file: str = "merged.parquet"
    ) -> None:
    """ ready to use function to merge multiple files into one
    parquet file. """

    if len(files) < 2:
        print("Need at least two files to merge")
        return None

    histories = (load_history(file, directory="processed")
                 for file in files)

    merged_history = Preprocessor._merge_histories(histories)
    save_history(merged_history, output_file, directory="processed")

    return None
