import asyncio
from web_scrapers import vsin_scraper


async def run():
    df = await vsin_scraper.get_vsin_game_lines()
    print(df)


def handler(event, context):
    asyncio.get_event_loop().run_until_complete(run())


if __name__ == "__main__":
    handler(event=None, context=None)
