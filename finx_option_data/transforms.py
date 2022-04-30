import numpy as np
import pandas as pd


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
        assert results == 1, "to many rows returned"
        return xdf.loc[matches.index[0]]

    # idf = tdf.copy()
    # for key, grouped in idf.groupby(["strike", "call_put"]):
    #     for idx, row in grouped.sort_values("lastTradingDay").iterrows():
    #         # optionally skip rows that will not match with dte_diff
    #         last_ew_row = find_matching_row(idf, row, dte_diff=3)
    #         idf.loc[idx, "matchingSymbol"] = np.NaN if last_ew_row is None else last_ew_row.name

    # iidf = idf.query("lastTradingDayDes == 'A1'")#.query("410 < strike and strike < 440")#.query("call_put == @call")
    # iidf[["lastTradingDayDes", "matchingSymbol"]]

    # iidf.matchingSymbol.isna().sum() 

    # # iidf


from math import ceil
def week_of_month(dt):
    """Returns the week of the month for the specified date."""
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + (1 + first_day.weekday()) % 7
    return str(int(ceil(adjusted_dom/7.0)))

def es_day_of_week(dt) -> str:
    """Returns the /ES week day convention"""
    es_weekday = {
        0: "A",
        1: "B",
        2: "C",
        3: "D",
        4: "EW",
    }[dt.weekday()]
    return es_weekday + str(week_of_month(dt))

def transform_last_trading_day_human(df):
    """Sets the op ex day to /ES conventions (eg, A1, 2C, EW4"""
    df["lastTradingDayDes"] = df["lastTradingDay"].apply(es_day_of_week)
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

def transform(df):
    df = transform_columns_datetime(df)
    print("transform_columns_datetime")
    
    df = transform_symbol_underlying(df)
    print("transform_symbol_underlying")
    
    df = transform_add_strike(df)
    print("transform_add_strike")
    
    df = transform_add_call_put(df)
    print("transform_add_call_put")

    df = transform_last_trading_day_human(df)
    print("transform_last_trading_day_human")
    
    return df

# tdf = transform(df) #.compute()


# def filter_df(df, 
#            underlyings: list=None, 
#            last_trading_day_min=None, 
#            last_trading_day_max=None
#     ):
    
#     if underlyings is not None:
#         df = df[ df['underlying'].isin(underlyings)].copy()
    
#     if last_trading_day_min is not None:
#         df = df[last_trading_day_min <= df["lastTradingDay"]].copy()
    
#     if last_trading_day_max is not None:
#         df = df[df["lastTradingDay"] <= last_trading_day_max].copy()
        
#     return df

# print(f"Columns include: {df.columns}")

# tdf = filter_df(df, 
#              underlyings=["SPY"], 
# #              last_trading_day_min=pd.to_datetime("2022-04-29"), 
# #              last_trading_day_max=pd.to_datetime("2022-05-3")
#             )

# # x = pd.Timestamp('2022-04-25 21:50')

# # tdf.query("@x <= sampleTimeInLong")

# x = pd.to_datetime('2022-04-25 21:00')

# # strike = str(430)

# tdf = (tdf.
#      query("@x <= sampleTimeInLong").
#     #  query("strike == @strike").
#      groupby("symbol").
#      agg({
         
#         "ask": "last", 
#         "bid": "last", 
#         "strike": "last", 
#         "call_put": "last", 
#         "delta": "last",
#         "lastTradingDay": "last", 
#         "quoteTimeInLong": "last",
#         "daysToExpiration": "last",
#         "volatility": "last",
#         "lastTradingDayDes": "last"

#     }).sort_values(["symbol"])
# ).copy()

# h = pd.merge(iidf, idf, left_on="matchingSymbol", right_index=True)
# (h.volatility_x - h.volatility_y).mean()

# # h.set_index("strike_y").volatility_y.hist()

# h.volatility_x.isna().sum() 