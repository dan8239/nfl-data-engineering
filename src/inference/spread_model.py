import importlib
import os

import joblib
import pandas as pd
import sportsdataverse as sdv

import config
from feature_eng import box_score_feature_adder, matchup_creator
from helpers import odds_convert
from preprocess import preprocess_box_scores, preprocess_tr_stats

importlib.reload(matchup_creator)
importlib.reload(preprocess_box_scores)
importlib.reload(box_score_feature_adder)
importlib.reload(config)


class SpreadModel:
    def __init__(self):
        model_path = "models/model_pipeline_20240911_204318.pkl"
        columns_path = "models/model_columns.pkl"
        stats_path = "../data/raw/tr_stats_short.xlsx"
        self.cache_path_template = "../output/predictions/{year}_{week}_predictions.csv"
        print(f"loading model from {model_path}")
        self.model = joblib.load(model_path)
        self.target_columns = joblib.load(columns_path)
        print(f"loading stats from {stats_path}")
        stats_db = pd.read_excel(stats_path)
        print("preprocessing stats df")
        stats_db = preprocess_tr_stats.preprocess(stats_db)
        self.mc = matchup_creator.MatchupCreator(
            stats_db=stats_db,
            games_to_sample=config.GAME_SAMPLE,
            aggregation_method=config.AGGREGATION_METHOD,
            decay_factor=config.DECAY_FACTOR,
        )
        self.training_features = config.SPREAD_MODEL_TRAINING_COLUMNS
        self.games_df = None
        self.output_cols = [
            "date",
            "home_short_display_name",
            "away_short_display_name",
        ]

    def __get_games_df(self, year, week):
        """_summary_

        Parameters
        ----------
        year : _type_
            _description_
        week : _type_
            _description_
        season_type : int
            2 for regular season, 3 for post season
        """
        games_df = sdv.espn_nfl_schedule(
            dates=year, week=week, limit=500, return_as_pandas=True
        )
        self.games_df = games_df
        print(games_df.shape)
        return games_df

    def __generate_stat_features(self, row):
        game_id = row["id"]
        home_short_display_name = row["home_short_display_name"]
        away_short_display_name = row["away_short_display_name"]
        date = row["date"]
        features_df = self.mc.create_matchup(
            game_id=game_id,
            home_team_box_short_display_name=home_short_display_name,
            away_team_box_short_display_name=away_short_display_name,
            date=date,
        )
        return features_df.iloc[0]

    def __preprocess(self, games_df):
        print("preprocessing games data")
        print(games_df.shape)
        games_df = preprocess_box_scores.preprocess(games_df)
        print(games_df.shape)
        bsfa = box_score_feature_adder.BoxScoreFeatureAdder(box_df=games_df)
        print("adding game features")
        games_df = bsfa.add_features(df=games_df, include_targets=False)
        print(games_df.shape)
        print("getting stats df")
        stats_df = games_df.apply(self.__generate_stat_features, axis=1)
        print(stats_df.shape)
        print("merging games w/ stats")
        preprocessed_df = pd.merge(games_df, stats_df, on="game_id", how="left")
        print(preprocessed_df.shape)
        preprocessed_df = preprocessed_df[self.training_features]
        return preprocessed_df

    def __postprocess(self, df):
        print("postprocessing request")
        meta_cols = [
            "date",
            "home_short_display_name",
            "away_short_display_name",
            "away_spread_result",
        ]
        meta_table = df[meta_cols]
        trans_table = df.drop(columns=meta_cols)
        transposed = trans_table.T.reset_index()
        transposed["index"] = transposed["index"].str.replace("_away_cover", "")
        transposed.rename(columns={"index": "away_spread"}, inplace=True)
        transposed["away_spread"] = transposed["away_spread"].astype(float)
        df_list = []
        for i, row in meta_table.iterrows():
            df = pd.DataFrame(transposed[["away_spread", i]])
            df.rename(columns={i: "away_cover_pcnt"}, inplace=True)
            df["home_team"] = row["home_short_display_name"]
            df["away_team"] = row["away_short_display_name"]
            df["away_spread_modeled"] = row["away_spread_result"]
            df["home_spread_modeled"] = df["away_spread_modeled"] * -1.0
            df["home_cover_pcnt"] = 1.0 - df["away_cover_pcnt"]
            df_list.append(df)
        reformatted_df = pd.concat(df_list, axis=0, ignore_index=True)
        reformatted_df["home_spread"] = reformatted_df["away_spread"] * -1.0
        for prefix in ["home", "away"]:
            reformatted_df[f"{prefix}_break_even_odds"] = reformatted_df[
                f"{prefix}_cover_pcnt"
            ].apply(odds_convert.percentage_to_us_odds)
        return reformatted_df

    def get_predictions(self, year, week, refresh_cache=False):
        """
        Parameters
        ----------
        year : int
            The year for which predictions are being made.
        week : int
            The week for which predictions are being made.
        refresh_cache : bool, optional
            If True, refreshes the cached predictions even if they exist, by default False.
        """

        cache_path = self.cache_path_template.format(year=year, week=week)

        # Check if cache exists and refresh_cache is False
        if os.path.exists(cache_path) and not refresh_cache:
            print(f"Loading predictions from cache: {cache_path}")
            return pd.read_csv(cache_path)

        print("Getting games_df")
        games_df = self.__get_games_df(year=year, week=week)
        X_new = self.__preprocess(games_df)

        print("Getting predictions")
        predictions = self.model.predict(X_new)
        predictions_df = pd.DataFrame(predictions, columns=self.target_columns)

        # Combine games_df with predictions
        games_df = games_df[self.output_cols]
        result_df = pd.concat(
            [games_df.reset_index(drop=True), predictions_df.reset_index(drop=True)],
            axis=1,
        )

        # Postprocess results
        result_df = self.__postprocess(result_df)

        # Save result to cache
        print(f"Saving predictions to cache: {cache_path}")
        result_df.to_csv(cache_path, index=False)

        return result_df
