from dotenv import dotenv_values
from loguru import logger
from os import getenv
from psycopg2 import Timestamp
import s3fs
import pandas as pd
import os

from finx_option_data.configs import Config


def download_parquet_file_raw(config: Config, dt: pd.Timestamp):
    fs = s3fs.S3FileSystem()

    root_path = f"finx-option-data/{dt.year}/{dt.month}/{dt.day}/"
    key = f"s3://{config.bucket_name}/{root_path}"

    parquet_files = fs.glob(f"{key}/*")

    for file in parquet_files:
        file_dir = os.path.dirname(os.path.realpath("__file__"))
        new_file_name = file.replace(
            f"{config.bucket_name}/finx-option-data/", f"{file_dir}/data/raw/"
        )
        new_path = "/" + "/".join(new_file_name.split("/")[1:-1])
        os.makedirs(new_path, exist_ok=True)
        fs.download(rpath=file, lpath=new_file_name, recursive=True)
        logger.info(f"Downloaded folder -- {file}")


if __name__ == "__main__":

    import os

    file_dir = os.path.dirname(os.path.realpath("__file__"))
    stage = os.getenv("STAGE", "prod").lower()
    file_name = f".env.{stage}"
    full_path = os.path.join(file_dir, f"./{file_name}")
    config = Config(full_path)

    dt: pd.Timestamp = pd.to_datetime("2022-4-26")
    download_parquet_file_raw(config, dt)
