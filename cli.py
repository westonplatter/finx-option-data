import click
from loguru import logger

from configure import authenticate_with_tda, write_example_env_file
from handler import (
    handler_check_pg_password,
    handler_fetch_data,
    handler_move_data_to_s3,
)
from handler_post_process import (
    handler_post_process_today,
    handler_post_process_yesterday,
)


@click.group()
def core():
    pass


@core.command()
def do_fetch_data():
    logger.debug("executing fetch data")
    handler_fetch_data(None, None)


@core.command()
def do_move_data_to_s3():
    logger.debug("executing move_data_to_s3")
    handler_move_data_to_s3(None, None)


@core.command()
def do_post_process_today():
    logger.debug("executing handler_post_process_today")
    handler_post_process_today(None, None)


@core.command()
def do_post_process_yesterday():
    logger.debug("executing handler_post_process_today")
    handler_post_process_yesterday(None, None)


@core.command()
def gen_env_sample():
    write_example_env_file()


@core.command()
def gen_tda_creds():
    authenticate_with_tda()


@core.command()
def get_secret():
    handler_check_pg_password(None, None)


if __name__ == "__main__":
    core()
