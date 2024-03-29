import time
from typing import Dict, Tuple, Union

import pandas as pd
import sqlalchemy as sa

from finx_option_data.configs import Config
from finx_option_data.models import StockQuote
from finx_option_data.utils import _market_schedule_between
from finx_option_data.polygon_helpers import open_close
from finx_option_data.data_ops import df_insert_do_nothing


class StockQuoteFetchAgent(object):
    """
    Client focused on intelligently fetching stock quote data
    """

    def __init__(self, polygon_api_key: str, engine: sa.engine, throttle_api_requests=True):
        self.polygon_api_key = polygon_api_key
        self.engine = engine
        self.throttle_api_requests = throttle_api_requests

    def fetch_price(self, ticker: str, dt: pd.Timestamp) -> Union[None, Dict]:
        if self.throttle_api_requests:
            time.sleep(20.0)
        date = dt.date()
        data = open_close(self.polygon_api_key, ticker, date)
        return data

    def ingest_price(self, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp) -> None:
        """
        Fetch stock price and store in database (if not already there)

        Args:
            ticker (str): stock ticker, e.g. "SPY"
            sd (pd.Timestamp): start date
            ed (pd.Timestamp): end date
        """
        df = self.calc_missing_prices(ticker, sd, ed)
        df['ticker'] = ticker
        df['open'] = None
        df['close'] = None
        df['high'] = None
        df['low'] = None
        df['fetched'] = None

        # for each row, fetch price and update DataFrame columns
        for ix, row in df.iterrows():
            data = self.fetch_price(ticker, row['dt'])
            
            if data is None:
                df.drop([ix]) # drop row if data is None
            
            row["open"] = data["open"]
            row["close"] = data["close"]
            row["high"] = data["high"]
            row["low"] = data["low"]
            row["fetched"] = True
            df.loc[ix] = row
        
        df_insert_do_nothing(df, self.engine, StockQuote)

    def query_prices(self, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp) -> pd.DataFrame:
        """
        Get stock prices from database between sd and ed
        """
        query = sa.text("select * from stock_quotes where :sd <= dt and dt <= :ed and ticker = :ticker and fetched = true")
        modified_sd = sd.replace(hour=0)
        modified_ed = ed.replace(hour=23)
        query_params = dict(table=StockQuote.__tablename__, sd=modified_sd, ed=modified_ed, ticker=ticker)
        with self.engine.connect() as con:
            return pd.read_sql(query, con=con, params=query_params)
        
    def calc_missing_prices(self, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp) -> pd.DataFrame:
        """
        Calculate missing stock prices from database between sd and ed
        """
        quotes = self.query_prices(ticker, sd, ed)
        schedule = _market_schedule_between(sd, ed)
        missing_dates = schedule[~schedule.market_close.isin(quotes.dt)]
        df = (missing_dates
            .reset_index()
            .drop('index', axis=1)
            .drop('market_open', axis=1)
            .rename(columns={"market_close": "dt"}))
        return df
