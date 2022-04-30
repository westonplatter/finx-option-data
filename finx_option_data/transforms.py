import numpy as np
import pandas as pd
from math import ceil


def find_matching_row(xdf, row, dte_diff: int = 0, strike_diff: float = 0.0, call_put_inverse: bool = False):
    """Finds matching option. Must be a single row result.

    Beware: optimized for speed, and not so much for readability.

    Args:
        xdf (pd.DataFrame): df to look in
        row (df row): option row
        dte_diff (int, optional): row.daysToExpiration - df.daysToExpiration. Defaults to 3 for matching Monday with previous Friday.
        strike_shift (float, optional): strike diff. Defaults to 0.0.
        call_put_inverse (bool, optional): True to inverse. Eg, True Call -> Put. Defaults to False.

    Returns:
        (pd.DataFrame): single row df
    """
    strike = row.strike + strike_diff
    cp = ("c" if row.call_put == "p" else "P") if call_put_inverse else row.call_put

    condition_cp = xdf["call_put"] == cp
    condition_strike = xdf["strike"] == strike
    condition_dte_diff = row["daysToExpiration"] - xdf["daysToExpiration"] == dte_diff

    matches = xdf[(condition_cp) & (condition_strike) & (condition_dte_diff)]

    results = len(matches)
    if results > 0:
        assert results == 1, "too many rows returned"
        return xdf.loc[matches.index[0]]


def _week_of_month(dt):
    """Returns the week of the month for the specified date."""
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + (1 + first_day.weekday()) % 7
    return str(int(ceil(adjusted_dom/7.0)))


def _es_day_of_week(dt) -> str:
    """Returns the /ES week day convention"""
    es_weekday = {
        0: "A",
        1: "B",
        2: "C",
        3: "D",
        4: "EW",
    }[dt.weekday()]
    return es_weekday + str(_week_of_month(dt))


def transform_last_trading_day_human(df):
    """Sets the op ex day to /ES conventions (eg, A1, 2C, EW4"""
    df["lastTradingDayDes"] = df["lastTradingDay"].apply(_es_day_of_week)
    return df


def transform_add_last_trading_day_week_number(df):
    df["lastTradingDayWeekNumber"] = df["lastTradingDay"].dt.strftime("%Y-%W")
    return df


def transform_columns_datetime(df):
    for c in ['expirationDate', 'lastTradingDay', 'quoteTimeInLong', 'tradeTimeInLong', 'sampleTimeInLong']:
        df[c] = pd.to_datetime(df['expirationDate'], unit='ms')
    return df


def transform_symbol_underlying(df):
    df["underlying"] = df.symbol.str.split("_", expand=True, n=1)[0]
    return df


def transform_add_strike(df):
    df["strike"] = df.symbol.str.extract(r"_.*[C|P](.*)")
    df["strike"] = pd.to_numeric(df["strike"])
    return df
    

def transform_add_call_put(df):
    df['call_put'] = np.where(df["symbol"].str.contains("C"), 'c', 'p')
    return df