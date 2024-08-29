import importlib

import numpy as np
import pandas as pd

from geo import calc_distance

importlib.reload(calc_distance)


class BoxScoreFeatureAdder:
    def __init__(self, box_df):
        self.raw_df = box_df
        self.processed_df = None
        stadium_df = pd.read_csv("reference/stadiums.csv")
        stadium_df["latitude"] = stadium_df["latitude"].astype(float)
        stadium_df["longitude"] = stadium_df["longitude"].astype(float)
        self.stadium_df = stadium_df

    def __add_location_info(self, df, prefix):
        if prefix is None:
            print("adding location info for venue")
            cols_to_add = ["latitude", "longitude", "timezone_shift", "timezone"]
            stad_df = self.stadium_df.copy()[["venue_id"] + cols_to_add]
            left_merge_col = "venue_id"
        elif (prefix == "home") | (prefix == "away"):
            print(f"adding location info for {prefix} team")
            cols_to_add = ["latitude", "longitude", "timezone_shift"]
            stad_df = self.stadium_df.copy()[["venue_id"] + cols_to_add]
            renamer = {col: f"{prefix}_{col}" for col in cols_to_add}
            stad_df.rename(columns=renamer, inplace=True)
            left_merge_col = f"{prefix}_venue_id"
        df = pd.merge(
            left=df,
            right=stad_df,
            how="left",
            left_on=left_merge_col,
            right_on="venue_id",
        )
        self.processed_df = df
        return df

    def __add_travel_distance(self, df):
        print("adding travel distance")
        for prefix in ["home", "away"]:
            df[f"{prefix}_travel_distance"] = calc_distance.compute_distances(
                lat1_series=df["latitude"],
                lon1_series=df["longitude"],
                lat2_series=df[f"{prefix}_latitude"],
                lon2_series=df[f"{prefix}_longitude"],
            )
        df["travel_delta"] = df["home_travel_distance"] - df["away_travel_distance"]
        return df

    def __add_days_rest(self, df):
        print("adding days rest")
        for team in ["home", "away"]:
            team_col = f"{team}_short_display_name"
            for i, row in df.iterrows():
                team_name = row[team_col]
                game_date = row["game_datetime"]
                print(f"{team_name}: game date {game_date}")

                # Filter games for the specific team before the current game date
                team_games = df[
                    (df["home_short_display_name"] == team_name)
                    | (df["away_short_display_name"] == team_name)
                ].copy()
                team_games = team_games[team_games["game_datetime"] < game_date]
                team_games = team_games.sort_values(
                    by="game_datetime", ascending=False
                ).head(1)
                print(f"previous game time = {team_games['game_datetime']}")

                # Calculate days rest and cumulative rest
                if not team_games.empty:
                    # Calculate days rest and cumulative rest
                    days_rest = (
                        game_date - team_games["game_datetime"].iloc[0]
                    ) / pd.Timedelta(days=1)
                else:
                    # Handle case where no previous games are found
                    days_rest = 200.0
                print(f"days_rest {days_rest}")
                print()

                # Apply calculated rest back to the main DataFrame
                df.loc[i, f"{team}_days_rest"] = days_rest
        return df

    def __add_box_totals(self, df):
        print("adding box totals")
        df["score_differential"] = df["home_score"] - df["away_score"]
        df["total_score"] = df["home_score"] + df["away_score"]
        return df

    def __add_spread_covers(self, df):
        print("adding spread results")
        for spread in np.arange(-20, 20.5, 0.5):
            print(spread)
            prefix = ""
            if spread > 0:
                prefix = "neg"
            df[f"{prefix}{spread}_home_cover"] = (
                df["score_differential"] + spread
            ) >= 0
        return df

    def __add_total_covers(self, df):
        print("adding totals results")
        for total in np.arange(20, 60, 0.5):
            df[f"{total}total_over_hits"] = df["total_score"] >= total
        return df

    def add_features(self, df):
        for prefix in [None, "home", "away"]:
            df = self.__add_location_info(df, prefix)
            if prefix is not None:
                df[f"{prefix}_timezones_traveled"] = (
                    df["timezone_shift"] - df[f"{prefix}_timezone_shift"]
                )
        df["timezones_traveled_delta"] = (
            df["home_timezones_traveled"] - df["away_timezones_traveled"]
        )
        print(df.isnull().mean())
        df = self.__add_travel_distance(df)
        df = self.__add_days_rest(df)
        df = self.__add_box_totals(df)
        df = self.__add_spread_covers(df)
        df = self.__add_total_covers(df)
        self.processed_df = df
        return df
