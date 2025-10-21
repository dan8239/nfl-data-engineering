#!/usr/bin/env python3
"""
Monitor the progress of the Tuesday backfill operation.
Checks S3 every minute until all expected Tuesdays are present.
"""

import sys
import os
import time
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from src.s3_io.s3_client import S3Client


def get_expected_tuesdays():
    """Get all expected Tuesdays for NFL seasons 2024-2025 and 2025 (through Oct 14)."""
    tuesdays = []

    # 2024-2025 Season: Aug 2024 - Feb 2025
    for year in [2024, 2025]:
        if year == 2024:
            months = range(8, 13)  # Aug-Dec
        else:
            months = [1, 2, 8, 9, 10]  # Jan-Feb, Aug-Oct

        for month in months:
            date = datetime(year, month, 1)
            # Find first Tuesday
            while date.weekday() != 1:
                date += timedelta(days=1)
            # Collect all Tuesdays in this month
            while date.month == month:
                if date.date() <= datetime(2025, 10, 14).date():  # Only through Oct 14, 2025
                    tuesdays.append(date.date())
                date += timedelta(days=7)

    return sorted(tuesdays)


def count_tuesdays_in_s3(s3_client, bucket):
    """Count how many Tuesdays are currently in S3."""
    found_tuesdays = set()

    # Check all months for 2024 and 2025
    for year in [2024, 2025]:
        for month in range(1, 13):
            s3_key = f"data/raw/team_rankings/year={year}/month={month:02d}/data.parquet"

            try:
                df = s3_client.read_dataframe_from_s3(bucket, s3_key, columns=['timestamp'])
                if df is not None and len(df) > 0:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    df = df[df['timestamp'].notna()]

                    for ts in df['timestamp'].unique():
                        if ts.tz is None:
                            ts = pd.Timestamp(ts).tz_localize('UTC')
                        et_ts = ts.tz_convert('America/New_York')
                        date = et_ts.date()
                        # Only add if it's a Tuesday
                        if date.weekday() == 1:
                            found_tuesdays.add(date)
            except Exception:
                continue

    return sorted(found_tuesdays)


def main():
    s3_client = S3Client()
    bucket = "djp-nfl-model"

    expected_tuesdays = get_expected_tuesdays()
    print(f"Expected Tuesdays: {len(expected_tuesdays)}")
    print("=" * 80)

    start_time = time.time()
    check_count = 0

    while True:
        check_count += 1
        elapsed = int(time.time() - start_time)

        print(f"\n[Check #{check_count}] Time elapsed: {elapsed // 60}m {elapsed % 60}s")
        print("-" * 80)

        found_tuesdays = count_tuesdays_in_s3(s3_client, bucket)
        missing_tuesdays = [t for t in expected_tuesdays if t not in found_tuesdays]

        print(f"Tuesdays found: {len(found_tuesdays)}/{len(expected_tuesdays)}")
        print(f"Missing: {len(missing_tuesdays)}")

        if missing_tuesdays:
            print(f"\nMissing Tuesdays ({len(missing_tuesdays)}):")
            # Group by month for easier reading
            by_month = {}
            for date in missing_tuesdays:
                key = f"{date.year}-{date.month:02d}"
                if key not in by_month:
                    by_month[key] = []
                by_month[key].append(date)

            for month_key in sorted(by_month.keys()):
                dates = by_month[month_key]
                print(f"  {month_key}: {len(dates)} missing - {[d.day for d in dates]}")
        else:
            print("\n" + "=" * 80)
            print("✓ ALL TUESDAYS COMPLETE!")
            print("=" * 80)
            print(f"\nTotal time: {elapsed // 60}m {elapsed % 60}s")
            break

        # Wait 60 seconds before next check
        if missing_tuesdays:
            print("\nWaiting 60 seconds before next check...")
            time.sleep(60)


if __name__ == "__main__":
    main()
