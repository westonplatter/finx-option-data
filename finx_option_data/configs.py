import os

from dotenv import dotenv_values
from sqlalchemy import create_engine
from finx_option_data.models import Base



class Config(object):
    def __init__(self, config_full_path: str):
        self.configs = {**self._get_file_configs(config_full_path), **os.environ}

    def _get_file_configs(self, config_full_path: str):
        if os.path.exists(config_full_path):
            return dotenv_values(config_full_path)
        else:
            return {}

    @property
    def bucket_name(self):
        return self.configs.get("BUCKET_NAME", None)

    @property
    def metrics_postgres_connstr(self):
        return self.configs.get("PG_CONNECTION_STR", None)

    @property
    def engine_metrics(self):
        conn_str = self.metrics_postgres_connstr
        engine = create_engine(conn_str, echo=True, future=True)
        Base.metadata.create_all(engine)
        return engine
    
    @property
    def polygon_api_key(self):
        return self.configs.get("POLYGON_API_KEY", None)
