import asyncio

from web_scrapers.team_rankings import predictive_rankings_scraper


async def run():
    # df = await vsin_scraper.get_vsin_game_lines()
    date = "10-15-2023"
    scr = predictive_rankings_scraper.PredictiveRankingsScraper()
    df = scr.get_table(date=date)
    print(df)


def handler(event, context):
    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    handler(event=None, context=None)
