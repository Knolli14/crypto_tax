import pandas as pd
from typing import Generator
from ctax.preprocess.cex.baseprocessor import BaseProcessor

class KucoinProcessor(BaseProcessor):

    @classmethod
    def preprocess(cls, history: pd.DataFrame) -> pd.DataFrame:
        print("\n-> Preprocessing Kucoin data...")

        #check if there are any swaps
        is_swap  = ~history.Symbol.str.contains("USDT")

        if any(is_swap):
            swaps, stables = cls._swap_stable_split(history, is_swap)
            processed_swaps = cls._process_swaps(swaps)

        else:
            stables = history.copy()
            swaps = None

        processed_stables = cls._remove_stable_from_symbol(stables)

        processed_history = (
            processed_stables if swaps is None else
            pd.concat([processed_stables, processed_swaps], ignore_index=True)
        )

        return (
            processed_history
            .drop(columns=["Filled Volume"])
            .assign(Stable="USDT")
            .assign(Side=lambda df: df.Side.str.lower())
        )

    @property
    def rename_dict(cls) -> dict:
        return {
            "Filled Time(UTC+02:00)": cls.final_column_labels["timestamp"],
            "Side": cls.final_column_labels["tx_type"],
            "Filled Amount": cls.final_column_labels["amount_asset"],
            "Symbol": cls.final_column_labels["asset"],
            "Filled Volume (USDT)": cls.final_column_labels["amount_base"],
            "Stable": cls.final_column_labels["base_asset"],
        }

    #@property
    #def load_keywords(cls) -> dict:
    #    return cls.config["preprocess"]["kucoin"]


    @classmethod
    def _remove_stable_from_symbol(cls, stable_df: pd.DataFrame) -> pd.DataFrame:
        """Removes the USDT string from the symbol column."""
        print("  -> Removing 'USDT' from symbol column...")

        # helper function
        _extract_first_symbols = lambda col: (
            symbol[0] for symbol in cls._split(col).values
        )

        # main logic
        symbols = _extract_first_symbols(stable_df.Symbol)

        stable_df["Symbol"] = [*symbols]

        return stable_df


    @staticmethod
    def _swap_stable_split(
            history: pd.DataFrame,
            is_swap: pd.Series
        ) -> tuple[pd.DataFrame]:
        """If there are any transactions that are swaps, this method separates
        them from the stable transactions. It returns two seperate DataFrames."""
        print("  -> Splitting swaps and stable transactions")

        # Token Swaps
        df_swaps = history[is_swap].copy()

        # Stable transactions
        df_stable = history[~is_swap].copy()

        return (df_swaps, df_stable)


    @staticmethod
    def _create_inner_txs(swap: pd.Series) -> pd.DataFrame:
        """"""

        # helper functions
        def _split_swap() -> pd.DataFrame:
            """"""
            nonlocal swap
            txs = (pd.DataFrame(swap).T
                   .explode(["Symbol", "Filled Amount"], ignore_index=True))

            return txs

        _reverse_order = lambda col_name: swap[col_name][::-1]


        # main logic
        swap = swap.copy()
        is_buy = swap.Side == "BUY"

        if is_buy:
            swap["Symbol"] = _reverse_order("Symbol")
            swap["Filled Amount"] = _reverse_order("Filled Amount")

        txs = _split_swap()

        txs.Side = ["SELL", "BUY"]

        return txs


    @staticmethod
    def _merge_amounts(swaps_df):
        """ """

        swaps = swaps_df.copy()
        amounts = zip(swaps["Filled Amount"], swaps["Filled Volume"])
        swaps["Filled Amount"] = [*amounts]

        return swaps


    @classmethod
    def _process_swaps(cls, swaps_df):
        """ """
        print("  -> Processing swaps...")

        swaps_merged_amounts = cls._merge_amounts(swaps_df)
        swaps_merged_amounts.Symbol = cls._split(swaps_merged_amounts.Symbol)

        inner_txs = (cls._create_inner_txs(swap)
                     for __, swap in swaps_merged_amounts.iterrows())

        return pd.concat(inner_txs, ignore_index=True)


    @staticmethod
    def _split(col):
        """"""
        return col.str.split("-")


def process_kucoin(df: pd.DataFrame) -> pd.DataFrame:
    """ """
    return KucoinProcessor.process(df)
# Compare this snippet from ctax/preprocess/cex/baseprocessor.py:
