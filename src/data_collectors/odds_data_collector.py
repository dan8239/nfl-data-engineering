import os
from datetime import datetime

import dotenv
from loguru import logger

from data_clients.odds import get_odds
from data_collectors import data_collector
from s3_io import s3_client

dotenv.load_dotenv()


class OddsDataCollector(data_collector.DataCollector):
    def __init__(self):
        self.s3c = s3_client.S3Client()
        self.bucket = os.environ.get("AWS_BUCKET_NAME", "")

    def collect(self, datetime):
        logger.info("getting odds")
        odds_df = get_odds.get_upcoming_nfl_odds()
        filename = f"data/raw/odds/{datetime.year}/{datetime.month}/{datetime.day}/{datetime.hour}_{datetime.minute}_odds.parquet"
        self.s3c.push_dataframe_to_s3(
            df=odds_df, bucket_name=self.bucket, s3_key=filename
        )


if __name__ == "__main__":
    odc = OddsDataCollector()
    dt = datetime.now()
    odc.collect(dt)
