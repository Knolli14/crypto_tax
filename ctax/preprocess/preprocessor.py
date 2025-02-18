
from pathlib import Path
from pandas import DataFrame

from ctax.config.config import get_supported_cex, load_config

from ctax.preprocess.raw import preprocess_raw_history
from ctax.preprocess.merge import merge_history

from ctax.data import load_history, save_history
from ctax.paths import DATA_DIR


class HistoryPreprocessor:
    config = load_config()
    supported_cex = get_supported_cex(config)

    def __init__(self, cex: str = None):

        if cex and cex not in self.supported_cex:
            raise ValueError(f"{cex} is not supported. Choose from {self.supported_cex}")
        self.cex = cex

        self.save = False
        self.file_name = None
        self.directory = "processed"
        self.save_format = "parquet"


    # methods
    def preprocess(
        self,
        data: DataFrame | Path | str,
        **save_kwargs
        ) -> DataFrame:
        """
        :param data: DataFrame containing raw history data or Path to csv file
        """

        if self.cex is None:
            raise ValueError(
                """
                You need to specify the exchange for preprocessing first.
                \nUse set_cex() method to do so.
                """
            )

        self.set_save_options(**save_kwargs)

        if not isinstance(data, DataFrame):
            data = load_history(data, raw=True, cex=self.cex, directory="csv")

        df_processed = preprocess_raw_history(data, self.cex)

        if self.save:
            self._save(df_processed, self.file_name)

        return df_processed


    def preprocess_directory(
        self,
        directory: str | Path,
        cexs: list[str],
        *,
        merge: bool = True,
        save: bool = True,
        new_file: bool = True
    ) -> None:

        # check for possible errors
        if not isinstance(cexs, list):
            cexs = [cexs]

        dir_path = DATA_DIR / directory
        if not dir_path.is_dir():
            raise ValueError(f"{dir_path} is not a directory")

        files = list(dir_path.glob("*.csv"))
        if len(cexs) != len(files):
            raise ValueError("Number of cexs and files do not match")


        # start of preprocessing
        processed_data = []
        for file, cex in zip(files, cexs):
            df = self.set_cex(cex).preprocess(file, save=save, new_file=new_file)
            processed_data.append(df)


        # merging process
        if merge:

            merged_data = merge_history(processed_data)

            if save:
                self._save(merged_data, "merged.parquet")

        return {
            "processed_data": processed_data,
            "merged_data": merged_data
        }


    # setters
    def set_cex(self, cex: str) -> 'HistoryPreprocessor':

        if cex not in self.supported_cex:
            raise ValueError(f"{cex} is not supported. Choose from {self.supported_cex}")
        self.cex = cex

        return self


    def set_save_options(
        self,
        save = False,
        *,
        file_name = None,
        directory = "processed",
        save_format = "parquet", # right now only parquet is supported
        new_file = False,
        ) -> 'HistoryPreprocessor':
        """
        Method for changing saving behaviour during preprocessing.

        :param save: set to True if you want to save the processed data
        :param file_name: name of the file to save, needs to be specified if save is True
        :param directory: subdirectory of datafolder where to save the file
        :param save_format: format to save the file in, right now only parquet is supported
        :param new_file: if False it will overwrite existing files with the same name
        :raises TypeError: if parameters are not of the correct type
        :raises ValueError: if save_format is not supported
        """

        if not isinstance(save, bool):
            raise TypeError("save must be a boolean")
        self.save = save

        if not isinstance(file_name, (str, Path)) and file_name is not None:
            raise TypeError("file_name must be a string or Path")
        if save and not file_name:
            self.file_name = f"{self.cex}_processed.{self.save_format}"
            print(f"file_name not specified, saving as {self.file_name}")
        else:
            self.file_name = file_name

        if not isinstance(directory, str):
            raise TypeError("directory must be a string")
        self.directory = directory

        if self.save_format not in ["parquet"]: # add other and move to config
            raise ValueError("save_format must be 'parquet' ")
        self.save_format = save_format

        if not isinstance(new_file, bool):
            raise TypeError("new_file must be a boolean")
        self.new_file = new_file

        return self


    # helper
    def _save(self, df: DataFrame, file_name: str | Path) -> None:
        save_history(
            df=df,
            file_name=file_name,
            directory=self.directory,
            new_file=self.new_file
        )
        return None

    # dunders
    def __repr__(self):
        return f"HistoryPreprocessor(cex='{self.cex}')"
