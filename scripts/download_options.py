from datetime import timedelta
import pandas as pd
import warnings

from finx_option_data.configs import Config

config = Config(".env.prod")

from finx_option_data.option_quote_fetch_agent import OptionQuoteFetchAgent
from finx_option_data.stock_quote_fetch_agent import StockQuoteFetchAgent

sagent = StockQuoteFetchAgent(
    polygon_api_key=config.polygon_api_key, engine=config.engine_metrics
)
sd = pd.to_datetime("2022-01-03")
ed = sd + timedelta(days=60)
prices = sagent.query_prices("SPY", sd, ed)
prices.sort_values("dt", inplace=True)

oagent = OptionQuoteFetchAgent(
    polygon_api_key=config.polygon_api_key, engine=config.engine_metrics
)


def round_to_5(x, base=5):
    return base * round(x / base)


# ignore by the divid by zero messages
warnings.filterwarnings(
    "ignore", message="divide by zero encountered in double_scalars"
)


for ix, row in prices.iterrows():
    close = row["close"]
    rounded_close = round_to_5(row["close"])
    relative_sd = row["dt"]

    print(f"close={close}. rounded_close={rounded_close}. relative_sd={relative_sd}")

    price_diff = 5
    price_increment = 5

    range_min = rounded_close - price_diff
    range_max = rounded_close  # + price_diff + price_increment

    for strike in [x for x in range(range_min, range_max, price_increment)]:
        print(f"strike={strike}", flush=True)
        for option_type in ["call", "put"]:
            contracts = oagent.fetch_options_contracts(
                relative_sd, "SPY", option_type, strike, dte=10
            )
            for contract in contracts:
                ticker, dt = contract["ticker"], sd
                oagent.ingest_prices_to_exp(ticker, dt)
                print(".", end="", flush=True)