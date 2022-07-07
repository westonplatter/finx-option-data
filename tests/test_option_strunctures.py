import pandas as pd

from finx_option_data.transforms import gen_friday_and_following_monday


def test_fm():
    input_df = pd.read_csv("tests/fm.csv")
    df = gen_friday_and_following_monday(input_df)
    assert df.shape[0] == 1
    assert df['desc'].values[0] == "3_calendar"

