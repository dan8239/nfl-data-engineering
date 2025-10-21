from datetime import datetime

import pandas as pd
import pytz

from data_collectors import odds_data_collector, team_rankings_data_collector


def run_odds_dc(datetime):
    odc = odds_data_collector.OddsDataCollector()
    odc.collect(datetime)


def run_tr_dc(datetime):
    trdc = team_rankings_data_collector.TeamRankingsDataCollector()
    trdc.collect(datetime)


collector_map = {
    "odds_data_collector": run_odds_dc,
    "team_rankings_data_collector": run_tr_dc,
}


def handler(event, context):
    collectors_to_run = event.get("collectors_to_run")
    date = event.get("date", None)
    if date:
        # When date is explicitly provided, use it as-is in Central timezone
        # to preserve the actual date (don't shift to previous day)
        dt = pd.to_datetime(date)
        dt_central = dt.tz_localize(pytz.timezone("US/Central"))
    else:
        dt_utc = datetime.now(pytz.utc)
        dt_central = dt_utc.astimezone(pytz.timezone("US/Central"))

    eligible_collectors = collector_map.keys()
    for collector in collectors_to_run:
        if collector in eligible_collectors:
            collector_map.get(collector)(dt_central)


if __name__ == "__main__":
    handler(
        event={
            "collectors_to_run": ["team_rankings_data_collector"],
            "date": "2024-11-18",
        },
        context=None,
    )
