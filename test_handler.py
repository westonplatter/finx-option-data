from datetime import date, datetime, timedelta
from unittest.mock import patch

from handler import gen_from_and_to_dates


def test_gen_from_and_to_dates_start_date():
    start_date, _ = gen_from_and_to_dates()
    assert start_date == datetime.now().date()


def test_gen_from_and_to_dates_end_date():
    with patch("handler.OPTIONS_SCAN_MAX_DTE", 10):
        _, end_date = gen_from_and_to_dates()
        actual_end_date = datetime.now().date() + timedelta(days=10)
        assert end_date == actual_end_date

    with patch("handler.OPTIONS_SCAN_MAX_DTE", 31):
        _, end_date = gen_from_and_to_dates()
        actual_end_date = datetime.now().date() + timedelta(days=31)
        assert end_date == actual_end_date
