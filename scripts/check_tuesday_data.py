"""
Check which Tuesdays are present in S3 data for 2024 and 2025
"""
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from datetime import datetime, timedelta
from src.s3_io.s3_client import S3Client


def get_all_tuesdays(year):
    """Get all Tuesdays for a given year"""
    tuesdays = []
    date = datetime(year, 1, 1)

    # Find first Tuesday
    while date.weekday() != 1:  # 1 = Tuesday
        date += timedelta(days=1)

    # Collect all Tuesdays in the year
    while date.year == year:
        tuesdays.append(date.date())
        date += timedelta(days=7)

    return tuesdays


def check_data_for_year_month(s3_client, bucket, data_type, year, month):
    """Load data for a specific year/month and return unique dates (UTC and ET)"""
    s3_key = f"data/raw/{data_type}/year={year}/month={month:02d}/data.parquet"

    try:
        df = s3_client.read_dataframe_from_s3(bucket, s3_key, columns=['timestamp'])
        if df is not None and len(df) > 0:
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            # Drop NaT values
            df = df[df['timestamp'].notna()]
            if len(df) > 0:
                # Get unique timestamps and their timezone info
                unique_timestamps = sorted(df['timestamp'].unique())

                # Return both timestamps and dates
                return unique_timestamps
        return []
    except Exception as e:
        # Only print if it's not a 404
        if "404" not in str(e):
            print(f"  Month {month:02d}: Error - {str(e)[:80]}")
        return []


def main():
    s3_client = S3Client()
    bucket = "djp-nfl-model"
    data_type = "team_rankings"  # Team rankings should run every Tuesday from Aug-March

    print(f"Checking Tuesday data in s3://{bucket}/data/raw/{data_type}/")
    print("=" * 80)

    # Check both 2024 and 2025
    for year in [2024, 2025]:
        print(f"\n{year} Analysis:")
        print("-" * 80)

        # Get all expected Tuesdays
        expected_tuesdays = get_all_tuesdays(year)
        print(f"Expected Tuesdays in {year}: {len(expected_tuesdays)}")

        # Collect all actual timestamps from all months
        all_timestamps = []
        # Check all 12 months for both years
        months_to_check = range(1, 13)

        print(f"\nChecking months: {list(months_to_check)}")
        for month in months_to_check:
            timestamps = check_data_for_year_month(s3_client, bucket, data_type, year, month)
            if timestamps:
                all_timestamps.extend(timestamps)
                print(f"  Month {month:02d}: {len(timestamps)} unique timestamps")

        # Process timestamps in both UTC and ET
        all_timestamps = sorted(set(all_timestamps))

        # Convert to dates in both UTC and Eastern Time
        utc_dates = []
        et_dates = []

        for ts in all_timestamps:
            # Ensure timestamp is timezone-aware (assume UTC if naive)
            if ts.tz is None:
                ts = ts.tz_localize('UTC')

            # Get UTC date
            utc_date = ts.date()
            utc_dates.append(utc_date)

            # Convert to Eastern Time
            et_ts = ts.tz_convert('America/New_York')
            et_date = et_ts.date()
            et_dates.append(et_date)

        utc_dates = sorted(set(utc_dates))
        et_dates = sorted(set(et_dates))

        # Find Tuesdays in both timezones
        actual_tuesdays_utc = [d for d in utc_dates if d.weekday() == 1]
        actual_tuesdays_et = [d for d in et_dates if d.weekday() == 1]

        # Find missing Tuesdays (using ET since EventBridge likely uses ET for NFL schedule)
        missing_tuesdays = [t for t in expected_tuesdays if t not in actual_tuesdays_et]

        if missing_tuesdays:
            print(f"\nMissing Tuesdays ({len(missing_tuesdays)}):")
            for tuesday in missing_tuesdays[:10]:  # Show first 10
                print(f"  - {tuesday}")
            if len(missing_tuesdays) > 10:
                print(f"  ... and {len(missing_tuesdays) - 10} more")
        else:
            print(f"\n✓ All {len(expected_tuesdays)} Tuesdays present!")

        # Show what we have
        if actual_tuesdays_et:
            print(f"\nTuesdays present (Eastern Time):")
            for tuesday in actual_tuesdays_et:
                print(f"  ✓ {tuesday}")

        # Show sample timestamps with both timezones
        if all_timestamps:
            print(f"\nSample timestamps (showing timezone conversion):")
            for ts in all_timestamps[:5]:
                if ts.tz is None:
                    ts = ts.tz_localize('UTC')
                et_ts = ts.tz_convert('America/New_York')
                utc_day = ts.strftime("%A")
                et_day = et_ts.strftime("%A")
                print(f"  UTC: {ts} ({utc_day}) -> ET: {et_ts} ({et_day})")


if __name__ == "__main__":
    main()
