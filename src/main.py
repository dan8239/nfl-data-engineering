import asyncio

from web_scrapers import team_rankings_scraper


async def run():
    # df = await vsin_scraper.get_vsin_game_lines()
    df = team_rankings_scraper.get_predictive_rankings()
    print(df)


def handler(event, context):
    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    handler(event=None, context=None)
