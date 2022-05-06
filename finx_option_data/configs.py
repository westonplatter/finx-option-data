import os

from dotenv import dotenv_values


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
