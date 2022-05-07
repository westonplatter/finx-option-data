import sys
import pandas as pd
import numpy as np
import os
import sys

sys.path.append("..")

from finx_option_data.transforms import (
    transform_last_trading_day_human,
    transform_add_last_trading_day_week_number,
    transform_columns_datetime,
    transform_symbol_underlying,
    transform_add_strike,
    transform_add_call_put,
)

cached_input_df = None


def get_df():
    global cached_input_df
    if cached_input_df is None:
        dirname = os.path.dirname(__file__)
        data_dir_path = os.path.join(dirname, "spy_test_data.parquet.gzip")
        df = pd.read_parquet(data_dir_path)
        cached_input_df = df
    else:
        pass

    return cached_input_df.copy()


def test_transform_columns_datetime():
    df = get_df()
    df = transform_columns_datetime(df)
    dt_columns = [
        "expirationDate",
        "lastTradingDay",
        "quoteTimeInLong",
        "tradeTimeInLong",
        "sampleTimeInLong",
    ]
    for dt_column in dt_columns:
        assert df[dt_column].dtype == np.dtype(
            "<M8[ns]"
        ), f"{dt_column} is not np.dtype('<M8[ns]')"


def test_transform_symbol_underlying():
    df = get_df()
    df = transform_symbol_underlying(df)
    assert "SPY" in df["underlying"].unique()


def test_transform_add_strike():
    df = get_df()
    df = transform_add_strike(df)
    assert 428.0 in df["strike"].unique()


def test_transform_add_call_put():
    df = get_df()
    df = transform_add_call_put(df)
    values_set = set(df["call_put"].unique())
    assert set(["c", "p"]) == values_set


def test_transform_last_trading_day_human():
    df = get_df()
    df = transform_columns_datetime(df)  # required for next method
    df = transform_last_trading_day_human(df)
    assert "EW3" in df["lastTradingDayDes"].unique()


def test_transform_add_last_trading_day_week_number():
    df = get_df()
    df = transform_columns_datetime(df)  # required for next method
    df = transform_add_last_trading_day_week_number(df)
    assert "2022-17" in df["lastTradingDayWeekNumber"].unique()
