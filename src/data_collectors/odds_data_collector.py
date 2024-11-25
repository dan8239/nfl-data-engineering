from loguru import logger

from data_clients.odds import get_odds
from data_collectors import data_collector


class OddsDataCollector(data_collector.DataCollector):
    def __init__():
        pass

    def collect(self, datetime):
        logger.info("getting odds")
        odds = get_odds.get_upcoming_nfl_odds()
        return odds
