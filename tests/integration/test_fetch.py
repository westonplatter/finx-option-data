import sys

sys.path.append("...")
import handler


def test_fetch_data():
    event, context = None, None
    res = handler.handler_fetch_data(event, context)
    assert (
        res["message"]
        == "Successfully fetched option data for: ['SPY', 'QQQ', 'IWM', 'EEM', 'TLT', 'GLD', 'USO', 'XLE', 'XLK', 'AAPL', 'AMZN']"
    )
