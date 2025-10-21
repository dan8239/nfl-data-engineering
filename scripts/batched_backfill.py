#!/usr/bin/env python3
"""
Backfill missing Tuesdays in batches to avoid race conditions.
Only triggers Lambdas for different months simultaneously.

Usage:
    python scripts/batched_backfill.py
    python scripts/batched_backfill.py --start-year 2024 --end-year 2025
    python scripts/batched_backfill.py --start-date 2024-08-01 --end-date 2025-10-14
"""
import sys
import os
import time
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from src.s3_io.s3_client import S3Client
from trigger_lambda import trigger_lambda


def get_nfl_season_months(year):
    """Get NFL season months for a given year (Aug-Mar)."""
    return list(range(8, 13)) + list(range(1, 4))


def get_expected_tuesdays(start_date=None, end_date=None, start_year=None, end_year=None):
    """
    Get all Tuesdays within a date range.

    Args:
        start_date: Start date (YYYY-MM-DD string or datetime)
        end_date: End date (YYYY-MM-DD string or datetime)
        start_year: Start year (will use Aug of this year)
        end_year: End year (will use current month of this year)

    Returns:
        Sorted list of Tuesday dates
    """
    # Determine date range
    if start_date and end_date:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    elif start_year and end_year:
        # NFL season: Aug of start_year through current month of end_year
        start_date = datetime(start_year, 8, 1).date()
        end_date = datetime.now().date()
    else:
        # Default: current NFL season (Aug of last year through today)
        today = datetime.now().date()
        current_year = today.year
        # If we're before August, start from previous year's August
        if today.month < 8:
            start_date = datetime(current_year - 1, 8, 1).date()
        else:
            start_date = datetime(current_year, 8, 1).date()
        end_date = today

    # Find all Tuesdays in the date range
    tuesdays = []
    current = start_date

    # Move to first Tuesday
    while current.weekday() != 1:  # 1 = Tuesday
        current += timedelta(days=1)

    # Collect all Tuesdays until end_date
    while current <= end_date:
        tuesdays.append(current)
        current += timedelta(days=7)

    return sorted(tuesdays)


def get_existing_tuesdays(s3_client, bucket, start_year=None, end_year=None):
    """
    Get existing Tuesday dates from S3.

    Handles two schemas:
    - New schema: has 'date' column (date we scraped) and 'timestamp' (when Lambda ran)
    - Old schema: only has 'timestamp' column (use ET conversion to get Tuesday)

    Args:
        s3_client: S3Client instance
        bucket: S3 bucket name
        start_year: Start year to check (defaults to 2 years ago)
        end_year: End year to check (defaults to current year)
    """
    if start_year is None:
        start_year = datetime.now().year - 2
    if end_year is None:
        end_year = datetime.now().year

    existing = set()

    for year in range(start_year, end_year + 1):
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
    parser = argparse.ArgumentParser(
        description="Backfill missing Tuesday data by triggering Lambda functions in batches"
    )
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Start year (will use Aug of this year as start)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="End year (will use today as end)"
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("AWS_BUCKET_NAME", "djp-nfl-model"),
        help="S3 bucket name (default: from AWS_BUCKET_NAME env or 'djp-nfl-model')"
    )

    args = parser.parse_args()

    s3_client = S3Client()

    print("Checking current Tuesday coverage...")
    print("=" * 80)

    # Get expected Tuesdays based on provided arguments
    expected = get_expected_tuesdays(
        start_date=args.start_date,
        end_date=args.end_date,
        start_year=args.start_year,
        end_year=args.end_year
    )

    # Determine year range for S3 check
    if expected:
        start_year = min(d.year for d in expected)
        end_year = max(d.year for d in expected)
    else:
        start_year = end_year = datetime.now().year

    existing = get_existing_tuesdays(s3_client, args.bucket, start_year, end_year)
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
    existing_after = get_existing_tuesdays(s3_client, args.bucket, start_year, end_year)
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
