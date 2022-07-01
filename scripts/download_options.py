from datetime import timedelta, date
from re import I
import pandas as pd

from finx_option_data.configs import Config
config = Config(".env.prod")

from finx_option_data.option_quote_fetch_agent import OptionQuoteFetchAgent
from finx_option_data.stock_quote_fetch_agent import StockQuoteFetchAgent

sagent = StockQuoteFetchAgent(polygon_api_key=config.polygon_api_key, engine=config.engine_metrics)
sd = pd.to_datetime("2022-01-03")
ed = sd + timedelta(days=60)
prices = sagent.query_prices("SPY", sd, ed)
prices.sort_values("dt", inplace=True)

oagent = OptionQuoteFetchAgent(polygon_api_key=config.polygon_api_key, engine=config.engine_metrics)


def round_to_5(x, base=5):
    return base * round(x/base)


for ix, row in prices.iterrows():
    close = row['close']
    rounded_close = round_to_5(row['close'])
    relative_sd = row['dt']

    print(f"close={close}. rounded_close={rounded_close}. relative_sd={relative_sd}")

    for strike in [x for x in range(rounded_close-30,rounded_close+35, 5)]:
        print(f"strike={strike}", flush=True)
        for option_type in ["call", "put"]:
            contracts = oagent.fetch_options_contracts(relative_sd, "SPY", option_type, strike, dte=60)
            for contract in contracts:
                ticker, dt = contract['ticker'], sd
                oagent.ingest_prices_to_exp(ticker, dt)
                print(".", end="", flush=True)
        