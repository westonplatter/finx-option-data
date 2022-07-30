import pandas as pd

from finx_option_data.utils import _market_days_between
from finx_option_data.option_structures import gen_ff_calendars, gen_fm_calendars, gen_mf_calendars
from finx_option_data.configs import Config


if __name__ == "__main__":

    config = Config(".env.prod")

    market_datetimes = _market_days_between(
        start_dt=pd.Timestamp("2022-1-03"),
        end_dt=pd.Timestamp("2022-5-31"),
    )

    for dt in market_datetimes:
        try:
            # gen_ff_calendars(config, dt.date())
            gen_mf_calendars(config, dt.date())
            print(dt)
        except Exception as e:
            print(f"issue with {dt}")
