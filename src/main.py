import asyncio

import pandas as pd
from date_functions import date_functions

from data_clients.team_rankings import team_rankings_scraper
from src.data_clients.box_scores import box_score_cllector


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
    gc = box_score_cllector.GameCollector()
    gc.collect()
