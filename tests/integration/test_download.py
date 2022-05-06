import sys

sys.path.append("...")

from finx_option_data.configs import Config
from finx_option_data.download_data import download_parquet_file_raw
import pandas as pd


def test_download_raw():
    import os

    file_dir = os.path.dirname(os.path.realpath("__file__"))
    stage = os.getenv("STAGE", "prod").lower()
    file_name = f".env.{stage}"
    full_path = os.path.join(file_dir, f"./{file_name}")
    config = Config(full_path)

    dt: pd.Timestamp = pd.to_datetime("2022-4-25")

    download_parquet_file_raw(config, dt)
