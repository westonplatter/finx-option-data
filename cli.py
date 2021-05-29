import click


@click.group()
def core():
    pass


@core.command()
def do_fetch_data():
    print(f"executing fetch data")
    from handler import handler_fetch_data

    handler_fetch_data(None, None)


@core.command()
def do_move_data_to_s3():
    print(f"executing move_data_to_s3")
    from handler import handler_move_data_to_s3

    handler_move_data_to_s3(None, None)


if __name__ == "__main__":
    core()
