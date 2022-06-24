from contextlib import contextmanager
from datetime import datetime
import time
from typing import List

from loguru import logger
import pandas_market_calendars as mcal
import pandas as pd


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


@contextmanager
def timeit_context(name):
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time
    logger.debug(f"{elapsed_time:0.2f} [{name}]")


def _market_schedule_between(
    start_dt: datetime, end_dt: datetime, calendar: str = "NYSE"
) -> pd.DataFrame:
    nyse = mcal.get_calendar(calendar)
    FORMAT_STR = "%Y-%m-%d"
    df = nyse.schedule(
        start_date=start_dt.strftime(FORMAT_STR), end_date=end_dt.strftime(FORMAT_STR)
    )
    return df


def _market_days_between(
    start_dt: datetime, end_dt: datetime, calendar: str = "NYSE"
) -> List:
    df = _market_schedule_between(start_dt, end_dt, calendar=calendar)
    return df["market_close"].to_list()

def _market_closes_between(
    start_dt: datetime, end_dt: datetime, calendar: str = "NYSE"
) -> List:
    df = _market_schedule_between(start_dt, end_dt, calendar=calendar)
    return df["market_close"].to_list()
