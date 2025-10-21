#!/usr/bin/env python3
"""
Backfill missing Tuesdays in batches to avoid race conditions.
Only triggers Lambdas for different months simultaneously.
"""
import sys
import os
import time
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from src.s3_io.s3_client import S3Client
from trigger_lambda import trigger_lambda


def get_expected_tuesdays():
    """Get all Tuesdays for NFL seasons (Aug-Feb, up to Oct 14 2025)."""
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
                if date.date() <= datetime(2025, 10, 14).date():
                    tuesdays.append(date.date())
                date += timedelta(days=7)

    return sorted(tuesdays)


def get_existing_tuesdays(s3_client, bucket):
    """Get existing Tuesday dates from S3.

    Handles two schemas:
    - New schema: has 'date' column (date we scraped) and 'timestamp' (when Lambda ran)
    - Old schema: only has 'timestamp' column (use ET conversion to get Tuesday)
    """
    existing = set()

    for year in [2024, 2025]:
        for month in range(1, 13):
            s3_key = f"data/raw/team_rankings/year={year}/month={month:02d}/data.parquet"

            try:
                # Read whole file to check schema
                df = s3_client.read_dataframe_from_s3(bucket, s3_key)
                if df is None or len(df) == 0:
                    continue

                # Check which column to use
                if 'date' in df.columns:
                    # New schema - use date column
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df = df[df['date'].notna()]
                    for d in df['date'].unique():
                        date = pd.Timestamp(d).date()
                        if date.weekday() == 1:  # Tuesday
                            existing.add(date)
                elif 'timestamp' in df.columns:
                    # Old schema - use timestamp (convert to ET to get Tuesday)
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    df = df[df['timestamp'].notna()]
                    for ts in df['timestamp'].unique():
                        if ts.tz is None:
                            ts = pd.Timestamp(ts).tz_localize('UTC')
                        et_ts = ts.tz_convert('America/New_York')
                        date = et_ts.date()
                        if date.weekday() == 1:  # Tuesday
                            existing.add(date)
            except Exception:
                # Silently skip missing months (404 errors expected)
                continue

    return sorted(existing)


def main():
    s3_client = S3Client()
    bucket = "djp-nfl-model"

    print("Checking current Tuesday coverage...")
    print("=" * 80)

    expected = get_expected_tuesdays()
    existing = get_existing_tuesdays(s3_client, bucket)
    missing = sorted([d for d in expected if d not in existing])

    print(f"Expected Tuesdays: {len(expected)}")
    print(f"Existing Tuesdays: {len(existing)}")
    print(f"Missing Tuesdays: {len(missing)}")
    print()

    if not missing:
        print("✓ All Tuesdays present!")
        return

    # Group missing dates by month to avoid race conditions
    by_month = defaultdict(list)
    for date in missing:
        month_key = f"{date.year}-{date.month:02d}"
        by_month[month_key].append(date)

    print("Missing Tuesdays by month:")
    for month in sorted(by_month.keys()):
        dates = by_month[month]
        print(f"  {month}: {[d.day for d in dates]}")
    print()

    # Create batches - each batch has at most one date per month
    batches = []
    max_batch_size = max(len(dates) for dates in by_month.values())

    for i in range(max_batch_size):
        batch = []
        for month in sorted(by_month.keys()):
            if i < len(by_month[month]):
                batch.append(by_month[month][i])
        if batch:
            batches.append(batch)

    print(f"Created {len(batches)} batches to avoid race conditions")
    print(f"Batch sizes: {[len(b) for b in batches]}")
    print()
    print("Starting backfill...")
    print()

    # Execute batches
    for batch_num, batch in enumerate(batches, 1):
        print(f"\n{'=' * 80}")
        print(f"Batch {batch_num}/{len(batches)}: {len(batch)} dates")
        print(f"{'=' * 80}")

        for date in batch:
            date_str = date.strftime("%Y-%m-%d")
            print(f"  Triggering Lambda for {date_str}...")
            try:
                trigger_lambda(date_str)
                time.sleep(1)  # Small delay between triggers
            except Exception as e:
                print(f"    ✗ Error: {e}")

        if batch_num < len(batches):
            print(f"\nWaiting 5 minutes for batch {batch_num} to complete...")
            time.sleep(300)  # Wait 5 minutes between batches

    print(f"\n{'=' * 80}")
    print("✓ All batches triggered!")
    print(f"{'=' * 80}")
    print("\nWaiting 5 minutes for final batch to complete, then verifying...")
    time.sleep(300)

    # Verify
    print("\nVerifying results...")
    existing_after = get_existing_tuesdays(s3_client, bucket)
    still_missing = sorted([d for d in expected if d not in existing_after])

    print(f"Tuesdays now present: {len(existing_after)}/{len(expected)}")
    if still_missing:
        print(f"Still missing {len(still_missing)} Tuesdays:")
        for date in still_missing:
            print(f"  - {date}")
    else:
        print("✓ All Tuesdays now present!")


if __name__ == "__main__":
    main()
