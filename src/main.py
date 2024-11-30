from datetime import datetime

import pytz

from data_collectors import odds_data_collector


def run_odds_dc(datetime):
    odc = odds_data_collector.OddsDataCollector()
    odc.collect(datetime)


collector_map = {"odds_data_collector": run_odds_dc}


def handler(event, context):
    collectors_to_run = event.get("collectors_to_run")
    utc_now = datetime.now(pytz.utc)
    central_now = utc_now.astimezone(pytz.timezone("US/Central"))

    eligible_collectors = collector_map.keys()
    for collector in collectors_to_run:
        if collector in eligible_collectors:
            collector_map.get(collector)(central_now)


if __name__ == "__main__":
    handler(event={"collectors_to_run": ["odds_data_collector"]}, context=None)
