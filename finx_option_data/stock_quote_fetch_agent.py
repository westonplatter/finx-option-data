import time
from typing import Dict, Tuple

import pandas as pd
import sqlalchemy as sa

from finx_option_data.configs import Config
from finx_option_data.models import StockQuote
from finx_option_data.utils import _market_schedule_between
from finx_option_data.polygon_helpers import open_close
from finx_option_data.data_ops import df_insert_do_nothing


class StockQuoteFetchAgent(object):
    """Client focused on intelligently fetching stock quote data
    """

    def __init__(self, config: Config, engine: sa.engine, throttle_api_requests=True):
        self.polygon_api_key = config.polygon_api_key
        self.engine = engine
        self.throttle_api_requests = throttle_api_requests

    def ingest_price(self, ticker, sd, ed):
        df = self.calc_missing_prices(ticker, sd, ed)
        df['ticker'] = ticker
        df['open'] = None
        df['close'] = None
        df['high'] = None
        df['low'] = None
        df['fetched'] = None

        import pdb; pdb.set_trace()

        for ix, row in df.iterrows():
            if self.throttle_api_requests:
                print(".", end="", flush=True)
                time.sleep(6.0)
            data = open_close(self.polygon_api_key, ticker, row['dt'].date())
            if data is None:
                continue
            row["open"] = data["open"]
            row["close"] = data["close"]
            row["high"] = data["high"]
            row["low"] = data["low"]
            row["fetched"] = True
            df.loc[ix] = row
        
        df_insert_do_nothing(df, self.engine, StockQuote)

    def query_prices(self, ticker, sd, ed) -> pd.DataFrame:
        query = sa.text("select * from stock_quotes where :sd <= dt and dt <= :ed and ticker = :ticker")
        modified_sd = sd.replace(hour=0)
        modified_ed = ed.replace(hour=23)
        query_params = dict(table=StockQuote.__tablename__, sd=modified_sd, ed=modified_ed, ticker=ticker)
        with self.engine.connect() as con:
            return pd.read_sql(query, con=con, params=query_params)
        
    def calc_missing_prices(self, ticker, sd, ed):
        quotes = self.query_prices(ticker, sd, ed)
        schedule = _market_schedule_between(sd, ed)
        missing_dates = schedule[~schedule.market_close.isin(quotes.dt)]
        df = (missing_dates
            .reset_index()
            .drop('index', axis=1)
            .drop('market_open', axis=1)
            .rename(columns={"market_close": "dt"}))
        return df





