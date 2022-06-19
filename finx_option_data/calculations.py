import pandas as pd


# def calc_front_back_mid_diff(df: pd.DataFrame) -> None:
#     """
#     Calc the mid diff between -AskBid_front + AskBid_back.
#     Stores in mid_[f|b|diff].
#     Returns: None
#     """
#     df["mid_f"] = (df["ask_f"] - df["bid_f"])
#     df["mid_b"] = (df["ask_b"] - df["bid_b"])
#     df["mid_diff"] = -df["mid_f"] + df["mid_b"]
#     return None


def calcset_front_back_volatility_diff(df: pd.DataFrame) -> None:
    """
    Calc the volatility diff between -iv_f + iv_b
    Stores in iv_diff.
    Returns: None
    """
    df["iv_diff"] = -df["iv_f"] + df["iv_b"]
    return None


def calc_front_back_delta_diff(df: pd.DataFrame) -> None:
    """
    Calc the delta diff between -delta_f + delta_b
    Stores in delta_diff
    Returns: None
    """
    df["delta_diff"] = - df["delta_f"] + df["delta_b"]
    return None


def calcset_front_back_theta_diff(df: pd.DataFrame) -> None:
    """
    Calc the delta diff between -delta_f + delta_b
    Stores in delta_diff
    Returns: None
    """
    df["theta_diff"] = -df["theta_f"] + df["theta_b"]
    return None