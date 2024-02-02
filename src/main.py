import asyncio

from web_scrapers.team_rankings import team_rankings_scraper


async def run():
    # df = await vsin_scraper.get_vsin_game_lines()
    date = "2023-10-15"
    scr = team_rankings_scraper.TeamRankingsScraper()
    df = scr.get_all_tables_for_date(date=date)
    print(df)


def handler(event, context):
    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    handler(event=None, context=None)
