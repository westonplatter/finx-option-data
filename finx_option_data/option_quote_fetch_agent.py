import time
from typing import Dict, Tuple, List, Union

from datetime import timedelta
import numpy as np
import pandas as pd
import sqlalchemy as sa

from finx_option_pricer.option import Option

from finx_option_data.models import OptionQuote
from finx_option_data.stock_quote_fetch_agent import StockQuoteFetchAgent
from finx_option_data.utils import _market_schedule_between, _market_closes_between
from finx_option_data.polygon_helpers import (
    aggs,
    open_close,
    reference_options_contract_by_ticker_asof,
)
from finx_option_data.polygon_helpers import reference_options_contracts
from finx_option_data.data_ops import df_insert_do_nothing, df_upsert


class OptionQuoteFetchAgent(object):
    """Client focused on intelligently fetching option quote data"""

    def __init__(
        self,
        polygon_api_key: str,
        engine: sa.engine,
        throttle_api_requests: bool = False,
    ):
        self.polygon_api_key = polygon_api_key
        self.engine = engine
        self.throttle_api_requests = throttle_api_requests

    def throttle_if_needed(self):
        if self.throttle_api_requests:
            time.sleep(20)
    
    def fetch_options_contracts(
        self,
        dt: pd.Timestamp,
        underlying_ticker: str,
        option_type: str,
        strike: float,
        dte: int,
    ) -> Union[None, List[Dict]]:
        """
        Fetch option contracts.
        Return list of dictionaries from polygon's API
        """
        return reference_options_contracts(
            api_key=self.polygon_api_key,
            underlying_ticker=underlying_ticker,
            option_type=option_type,
            strike=strike,
            as_of=dt.date(),
            exp_date_lte=(dt.date() + timedelta(days=dte)),
        )

    def fetch_options_contracts_df(
        self,
        dt: pd.Timestamp,
        underlying_ticker: str,
        option_type: str,
        strike: float,
        dte: int,
    ) -> Union[None, List[Dict]]:
        dicts = self.fetch_options_contracts(dt, underlying_ticker, option_type, strike, dte)
        if dicts is None:
            return None
        df = pd.DataFrame(dicts)
        df.expiration_date = pd.to_datetime(df.expiration_date)
        return df

    def fetch_option_quotes_between(self, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp):
        market_closes = _market_closes_between(sd, ed)
        now = pd.Timestamp.utcnow()
        market_closes = list(filter(lambda x: x <= now, market_closes))

        quotes = []

        for ts in market_closes:
            self.throttle_if_needed()
            data = open_close(self.polygon_api_key, ticker, ts)
            quotes.append(data)
        
        return quotes
    
    def ingest_prices_to_exp(self, ticker: str, dt: pd.Timestamp) -> None:
        """Fetch and save option prices to DB

        Args:
            ticker (str): ticker of underlying
            dt (pd.Timestamp): date to fetch prices for

        Returns:
            None
        """
        contract_details = reference_options_contract_by_ticker_asof(
            self.polygon_api_key, ticker, dt
        )

        exp_date = pd.to_datetime(contract_details["expiration_date"])

        # find missing option quotes
        market_closes = _market_closes_between(dt, exp_date)
        sd, ed = market_closes[0], market_closes[-1]
        ed_missing_quotes = ed - timedelta(days=1)
        df = self.calc_missing_quotes(ticker, sd, ed_missing_quotes)

        df["ticker"] = ticker
        df["underlying_ticker"] = contract_details["underlying_ticker"]
        # use the last market close as the expiration date b/c it has the time
        df["exp_date"] = ed
        df["strike"] = contract_details["strike_price"]
        df["option_type"] = contract_details["contract_type"]

        sagent = StockQuoteFetchAgent(
            polygon_api_key=self.polygon_api_key,
            engine=self.engine,
            throttle_api_requests=True,
        )

        stock_prices_df = sagent.query_prices(
            ticker=contract_details["underlying_ticker"], sd=dt, ed=ed_missing_quotes
        )

        for ix, row in df.iterrows():
            dt_date = row["dt"].date()

            data = open_close(self.polygon_api_key, ticker, dt_date)
            if data is None:
                row["fetched"] = True
            else:
                # raw data
                row["open"] = data["open"]
                row["close"] = data["close"]
                row["high"] = data["high"]
                row["low"] = data["low"]
                row["volume"] = data["volume"]
                row["pre_market"] = data["preMarket"]
                row["after_market"] = data["afterHours"]
                row["fetched"] = True
                # derived data
                row["dte"] = (row["exp_date"] - row["dt"]).days
                row["exp_date_weekday"] = row["exp_date"].date().weekday()

                # derived iv and greek data
                try:
                    spot = stock_prices_df[
                        stock_prices_df.dt == row["dt"]
                    ].close.values[0]
                except Exception as e:
                    raise (e)

                dte_frac = row["dte"] / 252
                option = Option(
                    S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=None
                )
                row["iv"] = option.iv(opt_value=row.close)
                option = Option(
                    S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=row["iv"]
                )
                # implied greek values
                row["delta"] = option.delta
                row["theta"] = option.theta
                row["gamma"] = option.gamma
                row["vega"] = option.vega

                # replace row with updated values
                df.loc[ix] = row
                print(".", end="", flush=True)

        # quality issue - replace inf with np.nan
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # upsert to db
        df_without_id = df[np.isnan(df.id)].drop(columns=["id"])
        if len(df_without_id.index) > 0:
            df_insert_do_nothing(
                df=df_without_id, engine=self.engine, model=OptionQuote
            )

        df_with_id = df[~np.isnan(df.id)]
        if len(df_with_id.index) > 0:
            df_upsert(df=df_with_id, engine=self.engine, model=OptionQuote)

    def calc_missing_quotes(
        self, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp
    ) -> pd.DataFrame:
        """Calculate missing quotes for a given ticker and start_date/end_date range.

        Returns:
            pd.DataFrame: df of missing quotes with columns: all
        """
        quotes = self.query_quotes_between(ticker, sd, ed)
        schedule = _market_schedule_between(sd, ed)
        schedule.rename(columns={"market_close": "dt"}, inplace=True)
        schedule.drop(columns=["market_open"], inplace=True)
        return pd.merge(quotes, schedule, how="right", left_on="dt", right_on="dt")

    def query_quotes_between(
        self, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp
    ) -> pd.DataFrame:
        """Query quotes between [sd, ed] where fetched is not null

        Returns:
            pd.DataFrame: df of quotes with columns: all
        """
        query = sa.text(
            """
            select * 
            from option_quotes 
            where 
                (:sd <= dt and dt <= :ed) 
                and ticker = :ticker 
                and (fetched is null or fetched = false)
        """
        )
        modified_sd = sd.replace(hour=0)
        modified_ed = ed.replace(hour=23)
        query_params = dict(sd=modified_sd, ed=modified_ed, ticker=ticker)
        with self.engine.connect() as con:
            return pd.read_sql(query, con=con, params=query_params)
