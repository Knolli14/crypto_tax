from pandas import DataFrame, concat

def merge_history(histories: list[DataFrame]) -> DataFrame:
    """ """
    return \
        concat(histories, axis=0, ignore_index=True). \
        sort_values(by="timestamp", ascending=True, ignore_index=True)
