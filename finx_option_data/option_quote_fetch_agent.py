from typing import Dict, Tuple

from datetime import timedelta
import pandas as pd
import sqlalchemy as sa

from finx_option_data.configs import Config
from finx_option_data.models import OptionQuote

class OptionQuoteFetchAgent(object):
    """Client focused on intelligently fetching option quote data
    """

    def __init__(self, config: Config, engine: sa.engine):
        self.polygon_api_key = config.polygon_api_key
        self.engine = engine
    
    def fetch_contracts_by_params_delta_range_and_dte(self, 
        underlying_ticker: str, dt: pd.Timestamp, delta_range: Tuple[float], dte: int, call_put: int = None
    ):
        # todo fetch close price on dt
        # todo fetch contract for ATM call and ATM put
        pass

    def fetch_options_contracts(self, dt, underlying_ticker, option_type, strike):
        from finx_option_data.polygon_helpers import reference_options_contracts
        contracts = reference_options_contracts(
            api_key=self.polygon_api_key,
            underlying_ticker=underlying_ticker, 
            option_type=option_type,
            strike=strike,
            as_of=dt.date(),
            exp_date_lte=(dt.date() + timedelta(days=30))
        )
        import pdb; pdb.set_trace()

    def ingest_prices_to_exp(self, ticker, dt: pd.Timestamp) -> None:
        pass

    def get_quote(self, ticker, dt):
        pass

    def fetch_quote(self, ticker, dte):
        pass

    @classmethod
    def calc_iv_and_greeks(cls, stuff: Dict):
        pass
    
