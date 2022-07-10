from datetime import date, datetime
from typing import Dict, List, Union

import pandas as pd
import requests

DATE_FORMAT = "%Y-%m-%d"


def reference_options_contract_by_ticker_asof(
    api_key: str, ticker: str, as_of: date
) -> Union[None, List[Dict]]:
    """Fetch contract by ticker & as_of date

    Args:
        api_key (str): _description_
        ticker (str): _description_
        as_of (date): _description_

    Returns:
        _type_: _description_
    """
    url = "https://api.polygon.io/v3/reference/options/contracts"
    url += f"/{ticker}"
    url += f"?apiKey={api_key}"
    url += f"&as_of={as_of.strftime(DATE_FORMAT)}"
    res = requests.get(url)

    if res.status_code == 200:
        return res.json()["results"]

    if res.status_code == 404:
        return None


def reference_options_contracts(
    api_key: str,
    underlying_ticker: str,
    option_type: str,
    strike: float,
    as_of: date,
    exp_date_lte: date,
    limit: int = None,
) -> Union[None, List[Dict]]:
    """Get listed option contracts between `as_of` date and `exp_date_lte`.

    Args:
        api_key (str): _description_
        underlying_ticker (str): _description_
        option_type (str): _description_
        strike (float): _description_
        as_of (date): _description_
        exp_date_lte (date): _description_

        limit (int). _description_. Default is None

    Returns:
        _type_: _description_
    """
    url = f"https://api.polygon.io/v3/reference/options/contracts?"
    url += f"underlying_ticker={underlying_ticker}"
    url += f"&contract_type={option_type}"
    url += f"&strike_price={strike}"
    url += f"&expired=false"
    if limit is not None:
        url += f"&limit={limit}"
    url += f"&apiKey={api_key}"
    url += f"&as_of={as_of.strftime(DATE_FORMAT)}"
    url += f"&expiration_date.lte={exp_date_lte.strftime('%Y-%m-%d')}"
    res = requests.get(url)

    if res.status_code == 200:
        return res.json()["results"]

    if res.status_code == 404:
        return None


def open_close(api_key, ticker: str, date: date) -> Union[None, Dict]:
    """Get Open/Close data.

    Args:
        api_key (str): api key
        ticker (str): ticker (stock or option)
        date (date): date

    Returns:
        Union[None, Dict]: None or dict with key/values
    """
    date = date.strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={api_key}"
    res = requests.get(url)

    if res.status_code == 200:
        return res.json()

    if res.status_code == 404:
        data = res.json()
        # ie, if there is no data for that day.
        # this happens for option contracts with no trades
        if data["status"] == "NOT_FOUND":
            return None
        # this can happen if rate throttled
        else:
            res.raise_for_status()


def aggs(
    api_key,
    ticker: str,
    multiplier: int,
    time_span: str,
    sd: datetime,
    ed: datetime,
    limit: int = 5000,
) -> pd.DataFrame:
    """Fetch aggregated data.

    Args:
        api_key (str): _description_
        ticker (str): _description_
        multiplier (int): _description_
        time_span (str): _description_
        sd (datetime): datetime instance, utc
        ed (datetime): datetime instance
        limit (int, optional): _description_. Defaults to 5000.

    Returns:
        pd.DataFrame: df with columns: ['v', 'vw', 'o', 'close', 'h', 'l', 'dt', 'n']
    """

    start_ts = int(sd.timestamp() * 1000)
    end_ts = int(ed.timestamp() * 1000)

    url = "https://api.polygon.io/v2/aggs"
    url += f"/ticker/{ticker}"
    url += f"/range/{multiplier}/{time_span}/{start_ts}/{end_ts}?sort=asc&limit={limit}&apiKey={api_key}"
    res = requests.get(url)

    json = res.json()

    if res.status_code == 200:
        df = pd.DataFrame(json["results"])
        df.drop(columns=["h", "l", "o"], inplace=True)
        df.rename(
            columns={
                "v": "volume",
                "vw": "volume_weighted_price",
                "c": "close",
                "t": "dt",
                "n": "transaction_number",
            },
            inplace=True,
        )
        df["dt"] = pd.to_datetime(df["dt"], unit="ms")
        return df

    if res.status_code == 404:
        return None


# import requests

# url = f"https://api.polygon.io/v2/aggs/ticker/O:SPY220107P00475000/range/10/minute/1641315600000/1641330000000?adjusted=true&sort=asc&limit=240&apiKey=KwW950PZLa1uz5n5sWmzMzzNQj6ZjQun"
# # res = requests.get(url)
# # print(res.status_code)
# # json = res.json()
# # print(json)
# # data = json["results"]

# # df = pd.DataFrame(data=data)
# # df['ts'] = ts
# # df['te'] = te
# # df['tdiff'] = (df['t'] - df['ts'])/1_000/3600
# # df['th'] = pd.to_datetime(df['t']* 1_000_000)

# # df

# config.polygon_api_key
