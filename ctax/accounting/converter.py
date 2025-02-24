from datetime import datetime, date
from pandas import DataFrame

from typing import Literal

from ctax.accounting.finance import download_historic_prices, ticker
from ctax import data



class FiatConverter:

    def __init__(self, base: str = "EUR", rates: dict[str, DataFrame] = None):
        self.base = base.upper()
        self.rates = rates or {}


    def add_rates(
        self,
        forex_fiat: str,
        start: str | datetime | date = date(2000,1,1),
        end: str | datetime | date = date.today()
        ) -> None:
        """ """

        try:
            rates = download_historic_prices(
                ticker(self.base, forex_fiat),
                start, end
            )

        except Exception as e:
            print(e)
            return None

        return self.rates.update({forex_fiat.upper(): rates})



    def save_rates(
        self,
        fiat: str = "",
        format: Literal["csv", "parquet"] = "parquet") -> None:
        """ """

        save_funcs = {
            "csv": data.save_
        }

        if fiat:
            forex = [fiat]
        else:
            forex = self.rates.keys()

        for forex_fiat in forex:
            self.rates[forex_fiat].to_csv(f"rates/{forex_fiat}.csv")



# index needs to be a datetime object
def _is_outdated(df):
    return df.index.max() < datetime.now()

def _rates_exist(rates, forex_fiat: str) -> bool:
    return forex_fiat in rates
