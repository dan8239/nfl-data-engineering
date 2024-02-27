import dotenv
import sportsdataverse

dotenv.load_dotenv()


class GameCollector:
    def __init__(self):
        print("test some shit jackass")
        test = sportsdataverse.nfl.espn_nfl_schedule(
            dates=2023, week=1, season_type=2, limit=20, return_as_pandas=True
        )
        print(test.head())

    def collect():
        print("test some shit jackass")
