import dotenv
import pandas as pd
import sportsdataverse as sdv

dotenv.load_dotenv()


class GameCollector:
    def __init__(self):
        pass

    def get_box_scores(self, year_list):
        """get box scores (or upcoming game info) for a certain number of calendar years

        Parameters
        ----------
        year_list : list(str)
            list of years to collect

        Returns
        -------
        pd.DataFrame
            dataframe w/ all box score data
        """
        all_seasons_list = []
        for year in year_list:
            print(f"collecting box scores for {year}")
            season = sdv.nfl.espn_nfl_schedule(dates=year, return_as_pandas=True)
            print(season.head())
            all_seasons_list.append(season)
        df = pd.concat(all_seasons_list)
        return df


if __name__ == "__main__":
    gc = GameCollector()
    df = gc.collect([2024])
    df.to_csv("../output/box_scores/odds.csv", index=False)
