import pandas as pd
import sqlalchemy as sa

from finx_option_data.configs import Config
from finx_option_data.data_ops import df_insert_do_nothing
from finx_option_data.models import StrategyTimespreads
from finx_option_data.transforms import (
    gen_friday_and_following_monday,
    gen_friday_and_following_friday,
    gen_monday_and_following_friday,
)
from finx_option_data.utils import _market_days_between



def gen_fm_calendars(config: Config, underlying_ticker: str, dt: pd.Timestamp) -> None:
    query = sa.text(
        """
        select * 
        from option_quotes 
        where 
            underlying_ticker = :underlying_ticker 
            and date(dt) = :dt
    """
    )

    df = pd.read_sql(
        query, con=config.engine_metrics, params={"dt": dt, "underlying_ticker": underlying_ticker}
    )

    ddf = gen_friday_and_following_monday(df)
    df_insert_do_nothing(ddf, config.engine_metrics, StrategyTimespreads)


def gen_ff_calendars(config: Config, underlying_ticker: str, dt: pd.Timestamp) -> None:
    query = sa.text(
        """
        select *
        from option_quotes
        where
            underlying_ticker = :underlying_ticker
            and date(dt) = :dt
    """
    )
    df = pd.read_sql(
        query, con=config.engine_metrics, params={"dt": dt, "underlying_ticker": underlying_ticker}
    )
    ddf = gen_friday_and_following_friday(df)
    df_insert_do_nothing(ddf, config.engine_metrics, StrategyTimespreads)


def gen_mf_calendars(config: Config, underlying_ticker: str, dt: pd.Timestamp) -> None:
    query = sa.text(
        """
        select *
        from option_quotes
        where
            underlying_ticker = :underlying_ticker
            and date(dt) = :dt
    """
    )
    df = pd.read_sql(
        query, con=config.engine_metrics, params={"dt": dt, "underlying_ticker": underlying_ticker}
    )
    ddf = gen_monday_and_following_friday(df)
    df_insert_do_nothing(ddf, config.engine_metrics, StrategyTimespreads)