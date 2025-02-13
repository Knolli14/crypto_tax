from pandas import to_datetime, DataFrame, Timestamp
from datetime import date, datetime
from typing import Literal


def convert_to_datetime(df):
    return to_datetime(df["timestamp"], utc=True)

def print_success(message:str) -> None:
    """ """
    print(
        '-'*50,
        message,
        '-'*50,
        sep="\n"
    )
    return None

def get_correct_date_format(
    tx_date: date|str,
    needs: Literal['str','date']='str'
    )-> str|date:
    """ """

    if needs == 'str' and isinstance(tx_date, date):
        return tx_date.strftime("%Y-%m-%d")

    elif needs == 'date' and isinstance(tx_date, str):
        return datetime.strptime(tx_date, "%Y-%m-%d").date()

    else:
        return tx_date


def get_first_date(df: DataFrame) -> Timestamp:
    return df.timestamp.min()
