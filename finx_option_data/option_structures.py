import pandas as pd
import sqlalchemy as sa

from finx_option_data.configs import Config
from finx_option_data.data_ops import df_insert_do_nothing
from finx_option_data.models import StrategyTimespreads
from finx_option_data.transforms import (
    gen_friday_and_following_monday,
    gen_friday_and_following_friday,
)
from finx_option_data.utils import _market_days_between


def gen_fm_calendars(config: Config, dt: pd.Timestamp) -> None:
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
        query, con=config.engine_metrics, params={"dt": dt, "underlying_ticker": "SPY"}
    )

    ddf = gen_friday_and_following_monday(df)
    df_insert_do_nothing(ddf, config.engine_metrics, StrategyTimespreads)


def gen_ff_calendars(config: Config, dt) -> None:
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
        query, con=config.engine_metrics, params={"dt": dt, "underlying_ticker": "SPY"}
    )
    ddf = gen_friday_and_following_friday(df)
    df_insert_do_nothing(ddf, config.engine_metrics, StrategyTimespreads)


if __name__ == "__main__":

    config = Config(".env.prod")

    market_datetimes = _market_days_between(
        start_dt=pd.Timestamp("2022-01-21"),
        end_dt=pd.Timestamp("2022-02-28"),
    )

    for dt in market_datetimes:
        try:
            gen_fm_calendars(config, dt.date())
            print(dt)
        except Exception as e:
            # Traceback (most recent call last):
            # File "finx_option_data/option_structures.py", line 55, in <module>
            #     gen_fm_calendars(config, dt.date())
            # File "finx_option_data/option_structures.py", line 24, in gen_fm_calendars
            #     ddf = gen_friday_and_following_monday(df)
            # File "/Users/vifo/work/lsc/finx-option-data/finx_option_data/transforms.py", line 139, in gen_friday_and_following_monday
            #     return generic_timespread_generator(input_df, dte_diff, back_weekday)
            # File "/Users/vifo/work/lsc/finx-option-data/finx_option_data/transforms.py", line 121, in generic_timespread_generator
            #     comp = pd.merge(df, df, left_on="id", right_on="front_option_id", suffixes=("_f", "_b"))
            # File "/Users/vifo/.pyenv/versions/finx-all/lib/python3.8/site-packages/pandas/core/reshape/merge.py", line 106, in merge
            #     op = _MergeOperation(
            # File "/Users/vifo/.pyenv/versions/finx-all/lib/python3.8/site-packages/pandas/core/reshape/merge.py", line 703, in __init__
            #     self._maybe_coerce_merge_keys()
            # File "/Users/vifo/.pyenv/versions/finx-all/lib/python3.8/site-packages/pandas/core/reshape/merge.py", line 1256, in _maybe_coerce_merge_keys
            #     raise ValueError(msg)
            import pdb

            pdb.set_trace()
