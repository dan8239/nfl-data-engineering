import asyncio
import os

import pandas as pd

from date_functions import date_functions
from web_scrapers.team_rankings import team_rankings_scraper


async def run():
    # df = await vsin_scraper.get_vsin_game_lines()
    wed_list = date_functions.filter_dates(
        start_date_str="2023-08-30", day_of_week="Wednesday"
    )
    team_df_list = []
    scr = team_rankings_scraper.TeamRankingsScraper()
    for wed in wed_list:
        date_str = wed.strftime("%Y-%m-%d")
        print(f"Loading for {date_str}")
        df = scr.get_all_tables_for_date(date=wed)
        team_df_list.append(df)
        print("Saving one week data")
        df.to_excel(f"../output/one_week_stats_{date_str}.xlsx", index=False)
    print("Appending to other tables")
    team_df = pd.concat(team_df_list, ignore_index=True)
    print(f"All Dates DF Shape: {team_df.shape}")
    print("Saving combined data")
    team_df.to_excel(f"../output/team_stats_{date_str}.xlsx", index=False)


def handler(event, context):
    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    # handler(event=None, context=None)
    df_list = []
    for file in os.listdir("../output"):
        if file.endswith("xlsx"):
            path = f"../output/{file}"
            print(f"reading {path}")
            df = pd.read_excel(path)
            df_list.append(df)
    df = pd.concat(df_list)
    df.drop_duplicates(subset=["date", "team"], inplace=True)
    df.sort_values(by=["date", "team"], ascending=False)
    df.to_excel("../output/full_stats.xlsx", index=False)
