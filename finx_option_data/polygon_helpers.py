from datetime import date
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


def aggs(api_key, ticker: str, multiplier: int, time_span: str, sd: date, ed: date):

    url = "https://api.polygon.io/v2/aggs"
    url += f"/ticker/{ticker}"
    url += f"/range/{multiplier}/{time_span}/{sd.strftime(DATE_FORMAT)}/{ed.strftime(DATE_FORMAT)}?sort=asc&limit=1&apiKey={api_key}"
    res = requests.get(url)

    if res.status_code == 200:
        df = pd.DataFrame(res.json()["results"])
        df.rename(columns={"c": "close", "dt": "t"}, inplace=True)
        df["dt"] = pd.to_datetime(df["dt"])
        return df

    if res.status_code == 404:
        return None
