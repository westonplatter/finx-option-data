from typing import Dict, Tuple, List, Union

from datetime import timedelta
import numpy as np
import pandas as pd
import sqlalchemy as sa
from pytz import timezone

from finx_option_pricer.option import Option

from finx_option_data.configs import Config
from finx_option_data.models import OptionQuote
from finx_option_data.stock_quote_fetch_agent import StockQuoteFetchAgent
from finx_option_data.utils import _market_schedule_between, _market_closes_between
from finx_option_data.polygon_helpers import (
    open_close,
    reference_options_contract_by_ticker_asof,
)
from finx_option_data.polygon_helpers import reference_options_contracts
from finx_option_data.data_ops import df_insert_do_nothing, df_upsert

tzet = timezone("US/Eastern")


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

    def fetch_contracts_by_params_delta_range_and_dte(
        self,
        underlying_ticker: str,
        dt: pd.Timestamp,
        delta_range: Tuple[float],
        dte: int,
        call_put: int = None,
    ):
        # todo fetch close price on dt
        # todo fetch contract for ATM call and ATM put
        pass

    def fetch_options_contracts(
        self,
        dt: pd.Timestamp,
        underlying_ticker: str,
        option_type: str,
        strike: float,
        dte: int,
    ) -> Union[None, List[Dict]]:
        """Fetch option contracts. Return list of dictionaries from polygon's API"""
        return reference_options_contracts(
            api_key=self.polygon_api_key,
            underlying_ticker=underlying_ticker,
            option_type=option_type,
            strike=strike,
            as_of=dt.date(),
            exp_date_lte=(dt.date() + timedelta(days=dte)),
        )

    def ingest_prices_to_exp(self, ticker, dt: pd.Timestamp) -> None:
        """Fetch and save option prices to DB"""
        contract_details = reference_options_contract_by_ticker_asof(
            self.polygon_api_key, ticker, dt
        )

        exp_date = pd.to_datetime(contract_details["expiration_date"])
        if exp_date.date().weekday() != 4:
            return

        # TODO - check if dt equals expiration date. Stop
        
        market_closes = _market_closes_between(dt, exp_date)
        sd, ed = market_closes[0], market_closes[-1]
        df = self.calc_missing_quotes(ticker, sd, ed)

        df["ticker"] = ticker
        df["underlying_ticker"] = contract_details["underlying_ticker"]
        df["exp_date"] = ed
        df["strike"] = contract_details["strike_price"]
        df['option_type'] = contract_details['contract_type']

        cols = [
            "open",
            "close",
            "high",
            "low",
            "volume",
            "pre_market",
            "after_market",
            "fetched",
            "iv",
            "delta",
            "theta",
            "vega",
            "gamma",
            "weekday",
            "dte",
        ]
        for col in cols:
            df[col] = None

        sagent = StockQuoteFetchAgent(
            polygon_api_key=self.polygon_api_key,
            engine=self.engine,
            throttle_api_requests=True,
        )

        stock_prices_df = sagent.query_prices(
            ticker=contract_details["underlying_ticker"], sd=dt, ed=ed
        )

        for ix, row in df.iterrows():
            date = row["dt"].date()
            
            data = open_close(self.polygon_api_key, ticker, date)
            if data is None:
                row["fetched"] = True
            else:
                row["dte"] = (row["exp_date"] - row["dt"]).days
                if row["dte"] == 0:
                    df.drop([ix]) # drop row if data is None
                    continue

                row['weekday'] = row['dt'].date().weekday()
                # raw data
                row["open"] = data["open"]
                row["close"] = data["close"]
                row["high"] = data["high"]
                row["low"] = data["low"]
                row["volume"] = data["volume"]
                row["pre_market"] = data["preMarket"]
                row["after_market"] = data["afterHours"]
                row["fetched"] = True

                # calc iv
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
                row["iv"] = option.iv(row.close)
                option = Option(
                    S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=row["iv"]
                )
                # everything based on IV
                row["delta"] = option.delta
                row["theta"] = option.theta
                row["gamma"] = option.gamma
                row["vega"] = option.vega

                # replace row with updated values
                df.loc[ix] = row
                print(".", end="", flush=True)

        # quality issue - replace inf with np.nan
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        df_upsert(df, self.engine, OptionQuote)

    def calc_missing_quotes(self, ticker, sd, ed):
        quotes = self.query_quotes_between(ticker, sd, ed)
        schedule = _market_schedule_between(sd, ed)
        missing_dates = schedule[~schedule.market_close.isin(quotes.dt)]
        df = (
            missing_dates.reset_index()
            .drop("index", axis=1)
            .drop("market_open", axis=1)
            .rename(columns={"market_close": "dt"})
        )
        return df

    def query_quotes_between(self, ticker, sd, ed):
        """Query quotes between [sd, ed] where fetched is not null"""
        query = sa.text(
            "select * from option_quotes where (:sd <= dt and dt <= :ed) and ticker = :ticker and fetched is not null"
        )
        modified_sd = sd.replace(hour=0)
        modified_ed = ed.replace(hour=23)
        query_params = dict(sd=modified_sd, ed=modified_ed, ticker=ticker)
        with self.engine.connect() as con:
            return pd.read_sql(query, con=con, params=query_params)

    def fetch_quote(self, ticker, dte):
        pass
