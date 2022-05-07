from contextlib import contextmanager
import time

from loguru import logger


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


@contextmanager
def timeit_context(name):
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time
    logger.debug(f'{elapsed_time:0.2f} [{name}]')