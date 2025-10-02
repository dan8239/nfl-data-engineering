"""
Migration script to consolidate daily partitioned parquet files into monthly partitions.

Old structure: data/raw/{source}/{year}/{month}/{day}/*.parquet
New structure: data/raw/{source}/year={year}/month={month}/data.parquet
"""

import os
from collections import defaultdict
from datetime import datetime

import dotenv
import pandas as pd
from loguru import logger

from s3_io import s3_client

dotenv.load_dotenv()


class S3DataMigrator:
    def __init__(self, source_bucket, destination_bucket, dry_run=True):
        self.s3c = s3_client.S3Client()
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.dry_run = dry_run

    def normalize_data_types(self, df):
        """
        Normalize data types across dataframe to prevent mixed-type issues.
        Converts numeric-looking object columns to float64, handles errors gracefully.
        """
        for col in df.columns:
            if df[col].dtype == 'object':
                # Try to convert to numeric
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    # If conversion fails, keep as string
                    df[col] = df[col].astype(str)
        return df

    def list_all_files_in_prefix(self, prefix):
        """List all files under a given S3 prefix."""
        try:
            paginator = self.s3c.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.source_bucket, Prefix=prefix)

            files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.parquet'):
                            files.append(obj['Key'])
            return files
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []

    def parse_old_structure_path(self, s3_key):
        """
        Parse old structure path to extract metadata.
        Example: data/raw/odds/2024/9/15/14_30_odds.parquet
        Returns: (source, year, month) or None if invalid
        """
        try:
            parts = s3_key.split('/')
            if len(parts) >= 5 and parts[0] == 'data' and parts[1] == 'raw':
                source = parts[2]
                year = int(parts[3])
                month = int(parts[4])
                return (source, year, month)
        except (ValueError, IndexError):
            pass
        return None

    def migrate_source(self, source_name):
        """Migrate all data for a specific source (e.g., 'odds', 'team_rankings')."""
        logger.info(f"Starting migration for source: {source_name}")

        # List all files for this source
        prefix = f"data/raw/{source_name}/"
        all_files = self.list_all_files_in_prefix(prefix)
        logger.info(f"Found {len(all_files)} files for {source_name}")

        # Group files by year/month
        monthly_groups = defaultdict(list)
        for file_key in all_files:
            parsed = self.parse_old_structure_path(file_key)
            if parsed:
                source, year, month = parsed
                monthly_groups[(year, month)].append(file_key)
            else:
                logger.warning(f"Skipping file with unexpected structure: {file_key}")

        logger.info(f"Grouped into {len(monthly_groups)} monthly partitions")

        # Process each monthly group
        for (year, month), file_list in sorted(monthly_groups.items()):
            self.migrate_monthly_partition(source_name, year, month, file_list)

    def migrate_monthly_partition(self, source_name, year, month, file_list):
        """Consolidate all files from a month into a single monthly partition."""
        new_key = f"data/raw/{source_name}/year={year}/month={month:02d}/data.parquet"

        logger.info(
            f"Processing {source_name} {year}/{month:02d}: "
            f"{len(file_list)} files -> {new_key}"
        )

        if self.dry_run:
            logger.info(f"[DRY RUN] Would consolidate {len(file_list)} files into {new_key}")
            logger.info(f"[DRY RUN] Sample files: {file_list[:3]}")
            return

        try:
            # Read all files for this month
            dfs = []
            for file_key in file_list:
                logger.info(f"Reading {file_key}")
                df = self.s3c.read_dataframe_from_s3(self.source_bucket, file_key)
                if df is not None and not df.empty:
                    # Add timestamp if not present (infer from filename or use placeholder)
                    if 'timestamp' not in df.columns:
                        df['timestamp'] = datetime(year, month, 15)  # Mid-month placeholder

                    # Normalize data types to prevent mixed-type issues
                    df = self.normalize_data_types(df)
                    dfs.append(df)

            if not dfs:
                logger.warning(f"No valid data found for {year}/{month:02d}")
                return

            # Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Combined {len(dfs)} files into {len(combined_df)} rows")

            # Deduplicate (keep last occurrence)
            original_len = len(combined_df)
            combined_df = combined_df.drop_duplicates(
                subset=[col for col in combined_df.columns if col != 'timestamp'],
                keep='last'
            )
            logger.info(f"Deduplicated: {original_len} -> {len(combined_df)} rows")

            # Write to new location
            self.s3c.push_dataframe_to_s3(combined_df, self.destination_bucket, new_key)
            logger.info(f"Successfully migrated to s3://{self.destination_bucket}/{new_key}")

        except Exception as e:
            logger.error(f"Error migrating {year}/{month:02d}: {e}")

    def migrate_all_sources(self):
        """Migrate all known data sources."""
        sources = ['odds', 'team_rankings']
        for source in sources:
            try:
                self.migrate_source(source)
            except Exception as e:
                logger.error(f"Error migrating source {source}: {e}")


if __name__ == "__main__":
    # Use same bucket for source and destination (migrating in place)
    bucket = "djp-nfl-model"

    # Set dry_run=True to preview what will happen
    # Set dry_run=False to actually perform the migration
    migrator = S3DataMigrator(bucket, bucket, dry_run=False)

    logger.info(f"Starting migration in bucket: {bucket}")
    logger.info("Old format: data/raw/source/YYYY/MM/DD/*.parquet")
    logger.info("New format: data/raw/source/year=YYYY/month=MM/data.parquet")
    logger.info("=" * 60)

    # Migrate all sources
    migrator.migrate_all_sources()

    logger.info("=" * 60)
    logger.info("Migration complete!")
    logger.info(f"Data consolidated in s3://{bucket} with year/month partitions")
