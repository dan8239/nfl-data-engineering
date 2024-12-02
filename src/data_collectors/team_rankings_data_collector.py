import os
from datetime import datetime

import dotenv
from loguru import logger

from data_clients.team_rankings import team_rankings_scraper
from data_collectors import data_collector
from s3_io import s3_client

dotenv.load_dotenv()


class TeamRankingsDataCollector(data_collector.DataCollector):
    def __init__(self):
        self.s3c = s3_client.S3Client()
        self.bucket = os.environ.get("AWS_BUCKET_NAME", "")
        self.trs = team_rankings_scraper.TeamRankingsScraper()

    def collect(self, datetime):
        logger.info("getting stats")
        df = self.trs.get_all_tables_for_date(datetime)
        filename = f"data/raw/team_rankings/{datetime.year}/{datetime.month}/{datetime.day}/{datetime.hour}_team_rankings.parquet"
        print(filename)
        self.s3c.push_dataframe_to_s3(df=df, bucket_name=self.bucket, s3_key=filename)


if __name__ == "__main__":
    trdc = TeamRankingsDataCollector()
    dt = datetime.now()
    trdc.collect(dt)
