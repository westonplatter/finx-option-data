from dotenv import dotenv_values
from tda.auth import client_from_manual_flow


def write_example_env_file():
    ENV_FILE_NAME = ".env"
    ENV_SAMPLE_FILE_NAME = (".env.sample",)

    env_var_keys = []
    with open(ENV_FILE_NAME, "r") as fin:
        lines = fin.readlines()
        for line in lines:
            env_var_key = line.split("=")[0]
            env_var_keys.append(env_var_key)

    with open(ENV_SAMPLE_FILE_NAME, "w") as fout:
        for env_var_key in env_var_keys:
            env_var_line = f"{env_var_key}=value\n"
            fout.write(env_var_line)


def authenticate_with_tda():
    configs = dotenv_values(".env")
    TDA_CLIENT_ID = configs["TDA_CLIENT_ID"]
    TDA_REDIRECT_URL = configs["TDA_REDIRECT_URL"]
    token_path = "tda_api_creds.json"
    client_from_manual_flow(TDA_CLIENT_ID, TDA_REDIRECT_URL, token_path)
