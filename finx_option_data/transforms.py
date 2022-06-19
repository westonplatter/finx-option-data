import numpy as np
import pandas as pd
from math import ceil


def _week_of_month(dt):
    """Returns the week of the month for the specified date."""
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + (1 + first_day.weekday()) % 7
    return str(int(ceil(adjusted_dom / 7.0)))


def _es_day_of_week(dt) -> str:
    """Returns the /ES weekday convention"""
    es_weekday = {
        0: "A",
        1: "B",
        2: "C",
        3: "D",
        4: "EW",
    }[dt.weekday()]
    return es_weekday + str(_week_of_month(dt))


# def transform_last_trading_day_human(df):
#     """Sets the op ex day to /ES conventions (eg, A1, 2C, EW4"""
#     df["lastTradingDayDes"] = df["lastTradingDay"].apply(_es_day_of_week)
#     return df


# def transform_add_last_trading_day_week_number(df):
#     df["lastTradingDayWeekNumber"] = df["lastTradingDay"].dt.strftime("%Y-%W")
#     return df


# def transform_columns_datetime(df):
#     datetime_cols = [
#         "expirationDate",
#         "lastTradingDay",
#         "quoteTimeInLong",
#         "tradeTimeInLong",
#         "sampleTimeInLong",
#     ]
#     for c in datetime_cols:
#         df[c] = pd.to_datetime(df["expirationDate"], unit="ms")
#     return df


# def transform_symbol_underlying(df):
#     df["underlying"] = df.symbol.str.split("_", expand=True, n=1)[0]
#     return df


# def transform_add_strike(df):
#     df["strike"] = df.symbol.str.extract(r"_.*[C|P](.*)")
#     df["strike"] = pd.to_numeric(df["strike"])
#     return df


# def transform_add_call_put(df):
#     df["call_put"] = np.where(df["symbol"].str.contains("C"), "c", "p")
#     return df

def find_matching_row(
    xdf,
    row,
    dte_diff: int = 0,
    strike_diff: float = 0.0,
    call_put_inverse: bool = False,
):
    """Finds matching option. Must be a single row result.

    Beware: optimized for speed, and not so much for readability.

    Args:
        xdf (pd.DataFrame): df to look in
        row (df row): option row
        dte_diff (int, optional): row.dte - df.dte. Defaults to 3 for matching Monday with previous Friday.
        strike_shift (float, optional): strike diff. Defaults to 0.0.
        call_put_inverse (bool, optional): True to inverse. Eg, True Call -> Put. Defaults to False.

    Returns:
        (pd.DataFrame): single row df
    """
    strike = row.strike + strike_diff
    cp = ("call" if row['option_type'] == "put" else "put") if call_put_inverse else row['option_type']

    condition_cp = xdf["option_type"] == cp
    condition_strike = xdf["strike"] == strike
    condition_dte_diff = row["dte"] - xdf["dte"] == dte_diff

    matches = xdf[(condition_cp) & (condition_strike) & (condition_dte_diff)]

    results = len(matches)
    if results > 0:
        assert results == 1, "too many rows returned"
        return xdf.loc[matches.index[0]]


def gen_friday_and_following_monday(input_df):
    """
    Generate df with pointers (id and polygon option tickers) for front and back options

    Input:
        input_df(pd.DataFrame): df

    Returns:
        df(pd.DataFrame): with columns = ["ticker_f", "ticker_b", "id_f", "id_b", "desc"]
    """
    dte_diff = 3
    description = f"fm{abs(3)}dte_calendar"
    back_weekday = 0

    assert len(list(set(input_df['dt'].values))) == 1, "there is more than 1 dt value"

    df = input_df.copy()
    df['front_option_id'] = None
    
    for _, grouped in df.groupby(["strike", "option_type"]):
        for idx, row in grouped.iterrows():
            # iterate through all the back (ie, not front) options
            if row['exp_date'].weekday() == back_weekday:
                # find the match front option
                last_ew_row = find_matching_row(df, row, dte_diff=dte_diff)
                if last_ew_row is not None:
                    # if we can find a matching front option, record id (primary key for db table)
                    df.loc[idx, "front_option_id"] = last_ew_row.id
        
    comp = pd.merge(df, df, left_on="id", right_on="front_option_id", suffixes=("_f", "_b"))
    comp["desc"] = description
    comp.rename(columns={"dt_f": "dt"}, inplace=True)
    return comp[["dt", "ticker_f", "ticker_b", "id_f", "id_b", "desc"]]