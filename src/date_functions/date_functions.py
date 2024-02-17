from datetime import datetime

import pandas as pd


def filter_dates(start_date_str, day_of_week):
    """_summary_

    Args:
        start_date_str (_type_): _description_
        input_day_of_week (_type_): _description_

    Returns:
        _type_: _description_
    """
    start_date = pd.to_datetime(start_date_str)
    date_index = pd.date_range(start_date, datetime.now(), freq="D")
    df = pd.DataFrame(index=date_index)
    df["day_of_week"] = df.index.day_name()
    df["in_season"] = ((df.index.month >= 8) | (df.index.month <= 2)).astype(int)
    filtered_dates = df[(df["day_of_week"] == day_of_week) & (df["in_season"] == 1)]
    return filtered_dates.index
