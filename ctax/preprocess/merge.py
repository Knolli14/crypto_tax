from pandas import DataFrame, concat
from ctax.preprocess.raw import post_process

def merge_history(histories: list[DataFrame]) -> DataFrame:
    """ """
    print("\n-> Merging data...")

    df_merged = \
        concat(histories, axis=0, ignore_index=True). \
        sort_values(by="timestamp", ascending=True, ignore_index=True). \
        pipe(post_process)

    return df_merged
