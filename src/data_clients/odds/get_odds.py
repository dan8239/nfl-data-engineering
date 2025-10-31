import os

import dotenv
import pandas as pd
import requests
from loguru import logger

dotenv.load_dotenv()

__api_key = os.environ.get("ODDS_API_KEY")
__base_url = "https://api.the-odds-api.com/v4/sports"


def __request_upcoming_nfl_odds_us():
    url = f"{__base_url}/americanfootball_nfl/odds/?apiKey={__api_key}&regions=us&markets=h2h,spreads,totals&oddsFormat=american"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    requests_used = response.headers.get("X-Requests-Used")
    requests_remaining = response.headers.get("X-Requests-Remaining")
    total_requests = response.headers.get("Requests")
    print(f"Requests Used This Query: {total_requests}")
    print(f"Requests Used this Month: {requests_used}")
    print(f"Requests Remaining: {requests_remaining}")
    return response.json()


def __request_upcoming_nfl_odds_us2():
    url = f"{__base_url}/americanfootball_nfl/odds/?apiKey={__api_key}&regions=us2&markets=h2h,spreads,totals&oddsFormat=american"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    requests_used = response.headers.get("X-Requests-Used")
    requests_remaining = response.headers.get("X-Requests-Remaining")
    total_requests = response.headers.get("Requests")
    print(f"Requests Used This Query: {total_requests}")
    print(f"Requests Used this Month: {requests_used}")
    print(f"Requests Remaining: {requests_remaining}")
    return response.json()


def __response_to_df(response):
    dct_list = []
    for game in response:
        for book in game.get("bookmakers", []):
            for market in book.get("markets", []):
                for outcome in market.get("outcomes", []):
                    row_dict = {
                        "game_id": game.get("id"),
                        "game_time": game.get("commence_time"),
                        "home_team": game.get("home_team"),
                        "away_team": game.get("away_team"),
                        "book": book.get("key"),
                        "market": market.get("key"),
                        "outcome": outcome.get("name"),
                        "price": outcome.get("price"),
                        "point": outcome.get("point"),
                    }
                    dct_list.append(row_dict)
    df = pd.DataFrame(dct_list)
    df["point"] = df["point"].fillna(0.0)
    df.sort_values(
        by=["game_time", "game_id", "outcome", "point", "price"],
        ascending=[True, True, True, False, False],
        inplace=True,
    )

    # Log market types found to help detect missing markets
    if not df.empty:
        markets_found = df['market'].unique()
        logger.info(f"Markets found in response: {list(markets_found)}")

        # Check for expected markets
        expected_markets = {'h2h', 'spreads', 'totals'}
        missing_markets = expected_markets - set(markets_found)
        if missing_markets:
            logger.warning(f"Expected markets missing from response: {missing_markets}")
    else:
        logger.warning("No odds data returned from API")

    return df


def get_upcoming_nfl_odds():
    response = __request_upcoming_nfl_odds_us()
    df = __response_to_df(response)
    response = __request_upcoming_nfl_odds_us2()
    df2 = __response_to_df(response)
    return pd.concat([df, df2]).reset_index(drop=True)


if __name__ == "__main__":
    df = get_upcoming_nfl_odds()
    print(df)
