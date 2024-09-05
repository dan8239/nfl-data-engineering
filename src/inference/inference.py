import importlib

import joblib
import pandas as pd
import sportsdataverse as sdv

import config
from feature_eng import box_score_feature_adder, matchup_creator
from preprocess import preprocess_box_scores, preprocess_tr_stats

importlib.reload(matchup_creator)
importlib.reload(preprocess_box_scores)
importlib.reload(box_score_feature_adder)
importlib.reload(config)


class SpreadModel:
    def __init__(self):
        model_path = "models/model_pipeline_20240904_221227.pkl"
        columns_path = "models/model_columns.pkl"
        stats_path = "../data/raw/tr_stats_short.xlsx"
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

    def get_predictions(self, year, week):
        """


        Parameters
        ----------
        year : _type_
            _description_
        week : _type_
            _description_
        """
        print("getting_games_df")
        games_df = self.__get_games_df(year=year, week=week)
        X_new = self.__preprocess(games_df)
        predictions = self.model.predict(X_new)
        predictions_df = pd.DataFrame(predictions, columns=self.target_columns)
        games_df = games_df[self.output_cols]
        result_df = pd.concat(
            [games_df.reset_index(drop=True), predictions_df.reset_index(drop=True)],
            axis=1,
        )
        return result_df
