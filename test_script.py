import pandas as pd

from finx_option_data.configs import Config
config = Config("./.env.prod")

from finx_option_data.stock_quote_fetch_agent import StockQuoteFetchAgent
sagent = StockQuoteFetchAgent(polygon_api_key=config.polygon_api_key, engine=config.engine_metrics)

# from finx_option_data.utils import chunks
# for dates in chunks(pd.date_range(start="2020-06-27", end="2022-06-27", freq="D"), 10):
#     for ticker in ["SPY", "USO", "QQQ", "IWM"]:
#         sd, ed = dates[0], dates[-1]
#         sagent.ingest_price(ticker, sd, ed)
#     print(f"\nstart={sd}, end={ed}")

from finx_option_data.option_quote_fetch_agent import OptionQuoteFetchAgent
oagent = OptionQuoteFetchAgent(polygon_api_key=config.polygon_api_key, engine=config.engine_metrics)
sd = pd.to_datetime("2022-01-03")
contracts = oagent.fetch_options_contracts(sd, "SPY", "call", 400, dte=60)
for contract in contracts:
    ticker, dt = contract['ticker'], sd
    oagent.ingest_prices_to_exp(ticker, dt)