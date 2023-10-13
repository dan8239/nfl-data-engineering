import ssl
from datetime import datetime

import pandas as pd

from date_functions import date_functions

ssl._create_default_https_context = ssl._create_unverified_context

predictive_ratings_url = (
    "https://www.teamrankings.com/nfl/ranking/predictive-by-other/?date={}"
)
now = datetime.now()
wednesdays_list = date_functions.get_wednesdays(
    start_month=9, start_year=2023, end_month=now.month, end_year=now.year
)


def strip_team_names(df):
    """

    Strip the " (W-L-T)" from team names

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    df["Team"] = df["Team"].str.replace(r"\s\(.*\)", "", regex=True)
    return df


def add_date(df, date):
    df["date"] = date
    return df


def lower_case_col_names(df):
    df.columns = map(str.lower, df.columns)
    return df


def add_prefixes_to_col_names(df, prefix, skip_cols):
    df = df.rename(
        columns={
            col: prefix + col if col not in skip_cols else col for col in df.columns
        }
    )
    return df


def get_predictive_rankings():
    test = (
        "https://www.teamrankings.com/nfl/ranking/predictive-by-other/?date=2023-09-01"
    )
    tables = pd.read_html(test)
    df = tables[0]
    df = add_date(df, "2023-09-01")
    df = strip_team_names(df)
    df = lower_case_col_names(df)
    df = df[["date", "team", "rank", "rating", "hi", "low", "last"]]
    df = add_prefixes_to_col_names(
        df=df, prefix="tr_predictive_rankings_", skip_cols=["date", "team"]
    )
    return df
