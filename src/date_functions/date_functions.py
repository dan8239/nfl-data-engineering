from datetime import datetime, timedelta


def get_wednesdays(start_month, start_year, end_month, end_year):
    """

    get all dates that are wednesday between certain year/months

    Args:
        start_month (int):
        start_year (int):
        end_month (int):
        end_year (int):

    Returns:
        list(str): list of wednesday dates
    """
    wednesdays = []
    current_date = datetime(start_year, start_month, 1)

    while current_date < datetime(end_year, end_month, 1):
        if current_date.weekday() == 2:  # Wednesday has the index 2
            wednesdays.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return wednesdays
