import dotenv
import pandas as pd
import sportsdataverse as sdv

dotenv.load_dotenv()


class GameCollector:
    def __init__(self):
        self.df = None

    def collect(self):
        all_seasons_list = []
        for year in range(2005, 2024):
            print(f"collecting {year}")
            season = sdv.nfl.espn_nfl_schedule(dates=year, return_as_pandas=True)
            print(season.head())
            all_seasons_list.append(season)
            df = pd.DataFrame(season)
            df.to_csv(f"../output/box_scores/{year}_box_scores.csv", index=False)
        self.df = pd.concat(all_seasons_list)
        print("saving full df")
        self.df.to_csv("../output/box_scores/all_box_scores.csv", index=False)


if __name__ == "__main__":
    gc = GameCollector()
    gc.collect()
