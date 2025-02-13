import yfinance as yf

from datetime import datetime, timedelta, date

from pandas import DataFrame, Series
from typing import Optional

from ctax.utils import get_correct_date_format
from ctax.utils import get_first_date


# querying yahoo finance for historice price
def download_historic_prices(
    symbol: str,
    start: datetime,
    end: datetime = datetime.now()
    )-> DataFrame:
    """ """
    return yf.download(symbol, start=start, end=end, progress=False)["Close"]


def get_conversion_rate(
    tx_date: date | str | Series, #TODO: ugly, change!
    pair: str = "USDT-EUR",
    conversion_rates: Optional[DataFrame] = None
    )-> float:
    """ """

    if isinstance(tx_date, Series):
        pair =  f"{tx_date["fiat"]}EUR=X"
        tx_date = tx_date["timestamp"].date()


    # you can pass a preexisting conversions to avoid querying yahoo finance
    if conversion_rates is not None:

        conversion_date = get_correct_date_format(tx_date)
        return conversion_rates.loc[conversion_date, pair]

    else:
        try:
            conversion_date = get_correct_date_format(tx_date, needs='date')
            return download_historic_prices(
                symbol=pair,
                start=conversion_date,
                end=conversion_date + timedelta(days=1)
            ).iloc[0,0]

        except IndexError:
            print(f"Could not find conversion rate for {pair} on {conversion_date}")
            return None


def prepare_rates(df, base_fiat):
    forex = _extract_foreign_fiat(base_fiat, df.fiat)
    first_date = get_first_date(df)

    return {
        forex_fiat: download_historic_prices(ticker(base_fiat, forex_fiat), first_date)
        for forex_fiat in forex
    }

def _extract_foreign_fiat(base_fiat: str, column: Series) -> set:
    return set(column) - {base_fiat}


def ticker(base_fiat: str, forex_fiat: str, **fiats) -> str:
    return f"{forex_fiat}{base_fiat}=X"
