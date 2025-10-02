import os
from datetime import datetime

import dotenv
import pandas as pd
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

        # Add collection timestamp to the data
        odds_df['timestamp'] = datetime

        # Use year/month partitioning
        s3_key = f"data/raw/odds/year={datetime.year}/month={datetime.month:02d}/data.parquet"

        # Read existing monthly data if it exists, then upsert
        try:
            existing_df = self.s3c.read_dataframe_from_s3(
                bucket_name=self.bucket, s3_key=s3_key
            )
            if existing_df is not None:
                # Combine and remove duplicates (keep latest)
                combined_df = pd.concat([existing_df, odds_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(
                    subset=[col for col in combined_df.columns if col != 'timestamp'],
                    keep='last'
                )
                odds_df = combined_df
        except Exception as e:
            logger.info(f"No existing file found or error reading: {e}. Creating new file.")

        self.s3c.push_dataframe_to_s3(
            df=odds_df, bucket_name=self.bucket, s3_key=s3_key
        )


if __name__ == "__main__":
    odc = OddsDataCollector()
    dt = datetime.now()
    odc.collect(dt)
