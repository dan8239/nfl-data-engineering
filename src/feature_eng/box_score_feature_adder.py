import importlib

import numpy as np
import pandas as pd
import pytz

from geo import calc_distance

importlib.reload(calc_distance)


class BoxScoreFeatureAdder:
    def __init__(self, box_df):
        self.raw_df = box_df
        self.processed_df = None
        stadium_df = pd.read_csv("reference/stadiums.csv")
        stadium_df["latitude"] = stadium_df["latitude"].astype(float)
        stadium_df["longitude"] = stadium_df["longitude"].astype(float)
        stadium_df["venue_id"] = stadium_df["venue_id"].astype(float)
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
            suffixes=("", "_drop"),
        )
        df.drop(columns=[col for col in df if col.endswith("_drop")], inplace=True)
        self.processed_df = df
        return df

    def __add_local_time(self, df):
        print("adding local time")
        df["local_gametime"] = df.apply(
            lambda row: row["game_datetime"].tz_convert(pytz.timezone(row["timezone"])),
            axis=1,
        )
        df["game_time_hrs"] = df["local_gametime"].apply(
            lambda x: (x.hour if pd.notna(x) else 0)
            + (x.minute if pd.notna(x) else 0) / 60
            + (x.second if pd.notna(x) else 0) / 3600
        )
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
                team_games = df[
                    (df["home_short_display_name"] == team_name)
                    | (df["away_short_display_name"] == team_name)
                ].copy()
                team_games = team_games[team_games["game_datetime"] < game_date]
                team_games = team_games.sort_values(
                    by="game_datetime", ascending=False
                ).head(1)
                if not team_games.empty:
                    # Calculate days rest and cumulative rest
                    days_rest = (
                        game_date - team_games["game_datetime"].iloc[0]
                    ) / pd.Timedelta(days=1)
                else:
                    # Handle case where no previous games are found
                    days_rest = 200.0
                # Apply calculated rest back to the main DataFrame
                df.loc[i, f"{team}_days_rest"] = days_rest
        df["rest_differential"] = df["home_days_rest"] - df["away_days_rest"]
        return df

    def __add_box_totals(self, df):
        print("adding box totals")
        df["score_differential"] = df["home_score"] - df["away_score"]
        df["total_score"] = df["home_score"] + df["away_score"]
        return df

    def __add_spread_covers(self, df):
        print("adding spread results")
        for spread in np.arange(20, -24.5, -0.5):
            prefix = ""
            if spread > 0:
                prefix = "+"
            df[f"{prefix}{spread}_home_cover"] = np.where(
                df["score_differential"] + spread > 0,
                1,
                np.where(df["score_differential"] + spread == 0, 0.5, 0),
            )
        return df

    def __add_total_covers(self, df):
        print("adding totals results")
        for total in np.arange(25, 75, 0.5):
            df[f"{total}_total_over_hits"] = np.where(
                df["total_score"] > total,
                1,
                np.where(df["total_score"] == total, 0.5, 0),
            )
        return df

    def add_features(self, df, include_targets=True):
        df["venue_id"].fillna(df["home_venue_id"], inplace=True)
        for prefix in [None, "home", "away"]:
            df = self.__add_location_info(df, prefix)
            if prefix is not None:
                df[f"{prefix}_timezones_traveled"] = (
                    df["timezone_shift"] - df[f"{prefix}_timezone_shift"]
                )
        df["timezones_traveled_delta"] = (
            df["home_timezones_traveled"] - df["away_timezones_traveled"]
        )
        df = self.__add_local_time(df)
        df = self.__add_travel_distance(df)
        df = self.__add_days_rest(df)
        if include_targets:
            df = self.__add_box_totals(df)
            df = self.__add_spread_covers(df)
            df = self.__add_total_covers(df)
        self.processed_df = df
        return df
