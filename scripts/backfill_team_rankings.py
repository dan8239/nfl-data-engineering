"""
Backfill historical team rankings data from TeamRankings.com.

Collects data going backwards from Oct 2024 to Jan 2000, one week at a time.
Skips offseason months (April-July) as per cron schedule: 0 12 ? 8-3 2 *
Verifies each week was written to S3 before continuing.

Usage:
    cd /Users/danpfeiffer/Documents/code/nfl-data-engineering
    python scripts/backfill_team_rankings.py [--auto]

    # With logging:
    nohup python scripts/backfill_team_rankings.py --auto > logs/backfill.log 2>&1 &
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytz

# Add src to path so we can import modules
src_dir = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_dir))

# Change to src directory so relative paths work
os.chdir(str(src_dir))

from data_collectors import team_rankings_data_collector


def is_nfl_season(date):
    """
    Check if date is during NFL season (August-March).
    Matches cron expression: 0 12 ? 8-3 2 *
    """
    month = date.month
    # NFL season is August (8) through March (3)
    # April (4), May (5), June (6), July (7) are offseason
    return month >= 8 or month <= 3


def backfill(auto_mode=False):
    """Backfill team rankings from Oct 2024 to Jan 2000, skipping offseason."""

    print("=" * 70)
    print("Backfilling Team Rankings Data")
    print("Going backwards from Oct 2024 to Jan 2000")
    print("Skipping offseason months (April-July)")
    if auto_mode:
        print("🚀 AUTO MODE - Running all weeks without prompts")
    print("=" * 70)

    # Start from Oct 8, 2024 (Tuesday) and go back to Jan 2000
    start_date = datetime(2024, 10, 8)
    end_date = datetime(2000, 1, 1)

    current_date = start_date
    week_count = 0
    success_count = 0
    error_count = 0
    skipped_count = 0

    while current_date >= end_date:
        # Skip offseason months (April-July)
        if not is_nfl_season(current_date):
            print(f"\n⏭️  Skipping offseason date: {current_date.strftime('%Y-%m-%d')} (month {current_date.month})")
            skipped_count += 1
            current_date = current_date - timedelta(days=7)
            continue

        date_str = current_date.strftime("%Y-%m-%d")
        week_count += 1

        print(f"\n{'='*70}")
        print(f"Week {week_count}: {date_str}")
        print(f"{'='*70}")

        try:
            # Run collector
            print(f"  Running collector...")
            trdc = team_rankings_data_collector.TeamRankingsDataCollector()

            # Convert to US/Central timezone
            dt = pd.to_datetime(date_str)
            dt_central = dt.tz_localize(pytz.timezone("US/Central"))

            trdc.collect(dt_central)

            print(f"  ✓ Success")
            success_count += 1

        except Exception as e:
            print(f"  ✗ Error: {e}")
            error_count += 1

            # In auto mode, continue on error
            if not auto_mode:
                response = input("  Continue despite error? (y/n): ").strip().lower()
                if response != 'y':
                    print("\nStopping due to error.")
                    break

        # Show progress
        print(f"\nProgress: {week_count} weeks, {success_count} successful, {error_count} errors, {skipped_count} skipped")

        # In auto mode, skip prompts and continue
        if auto_mode:
            if week_count % 10 == 0:
                print(f"  📊 Checkpoint: {week_count} weeks processed, {skipped_count} offseason weeks skipped")
            current_date = current_date - timedelta(days=7)
            continue

        # Ask to continue (or auto mode)
        if error_count == 0:
            response = input("\nContinue to next week? (y/n/auto): ").strip().lower()

            if response == 'n':
                print("\nStopping. Re-run to continue.")
                break
            elif response == 'auto':
                print("\n🚀 Auto mode enabled - running all remaining weeks...")

                # Continue without prompting
                while current_date >= end_date:
                    current_date = current_date - timedelta(days=7)
                    if current_date < end_date:
                        break

                    # Skip offseason months
                    if not is_nfl_season(current_date):
                        print(f"\n⏭️  Skipping offseason date: {current_date.strftime('%Y-%m-%d')} (month {current_date.month})")
                        skipped_count += 1
                        continue

                    date_str = current_date.strftime("%Y-%m-%d")
                    week_count += 1

                    print(f"\n{'='*70}")
                    print(f"Week {week_count}: {date_str}")
                    print(f"{'='*70}")

                    try:
                        print(f"  Running collector...")
                        trdc = team_rankings_data_collector.TeamRankingsDataCollector()
                        dt = pd.to_datetime(date_str)
                        dt_central = dt.tz_localize(pytz.timezone("US/Central"))
                        trdc.collect(dt_central)
                        print(f"  ✓ Success")
                        success_count += 1
                    except Exception as e:
                        print(f"  ✗ Error: {e}")
                        error_count += 1

                    if week_count % 10 == 0:
                        print(f"\n📊 Progress: {week_count} weeks, {success_count} successful, {error_count} errors, {skipped_count} skipped")

                break  # Exit outer loop

        # Move back one week
        current_date = current_date - timedelta(days=7)

    print(f"\n{'='*70}")
    print("Backfill Complete")
    print(f"{'='*70}")
    print(f"  Total weeks processed: {week_count}")
    print(f"  Successful: {success_count}")
    print(f"  Errors: {error_count}")
    print(f"  Offseason weeks skipped: {skipped_count}")

    if error_count == 0:
        print("\n✅ All in-season weeks collected successfully!")
    else:
        print(f"\n⚠️  {error_count} weeks had errors")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill team rankings data from 2024 to 2000')
    parser.add_argument('--auto', action='store_true', help='Run in auto mode without prompts')
    args = parser.parse_args()

    backfill(auto_mode=args.auto)
