import numpy as np
import pandas as pd

from data_collectors.odds import get_odds
from helpers import get_team_id, odds_convert
from inference import spread_model


class BetRecommender:
    def __init__(
        self,
        stack_amount=5000,
        ev_dilution_factor=0.3,
    ):
        self.spread_model = spread_model.SpreadModel()
        self.pred_df = None
        self.odds_df = None
        self.ev_table = None
        self.best_bets = None
        self.books = []
        self.my_books = ["betrivers", "draftkings", "espnbet"]
        self.ev_dilution_factor = ev_dilution_factor
        self.stack_amount = stack_amount
        team_ids = team_ids = get_team_id.get_team_mapper()
        self.tr_to_odds_team_name_mapper = dict(
            zip(team_ids["box_short_display_name"], team_ids["odds_api_team_name"])
        )
        self.odds_to_tr_team_name_mapper = dict(
            zip(team_ids["odds_api_team_name"], team_ids["box_short_display_name"])
        )
        self.odds_to_tr_team_name_mapper["Washington Commanders"] = "Commanders"
        self.best_bet_cols = [
            "game_time",
            "away",
            "spread_modeled",
            "spread",
            "home",
            "outcome",
            "price",
            "point",
            "cover_pcnt",
            "expected_value",
            "ideal_bet_pcnt",
            "ideal_bet_amount",
            "ideal_bet_amount_diluted",
            "book",
        ]

    def __get_books(self, odds_df):
        return sorted(list(odds_df["book"].unique()))

    def get_predictions(self, year, week, refresh_predictions=False):
        if refresh_predictions or (self.pred_df is None):
            pred_df = self.spread_model.get_predictions(
                year=year, week=week, refresh_cache=refresh_predictions
            )
            pred_df = self.__map_team_names(pred_df)
            self.pred_df = pred_df
        else:
            return self.pred_df

    def __map_team_names(self, df):
        df["home_odds_name"] = df["home_team"].map(self.tr_to_odds_team_name_mapper)
        df["away_odds_name"] = df["away_team"].map(self.tr_to_odds_team_name_mapper)
        df["home"] = df["home_team"]
        df["away"] = df["away_team"]
        return df

    def get_odds(self, refresh_odds):
        if refresh_odds or self.odds_df is None:
            odds_df = get_odds.get_upcoming_nfl_odds()
            self.odds_df = odds_df
        self.books = self.__get_books(self.odds_df)
        return self.odds_df

    def __merge_to_ev_table(self):
        if (self.odds_df is None) or (self.pred_df is None):
            raise Exception("Need to get preds or get odds first")
        else:
            pred_home_cols = [
                "home_odds_name",
                "away_odds_name",
                "home",
                "away",
                "home_cover_pcnt",
                "home_spread_modeled",
                "home_spread",
                "home_break_even_odds",
            ]
            pred_away_cols = [
                "home_odds_name",
                "away_odds_name",
                "home",
                "away",
                "away_spread",
                "away_spread_modeled",
                "away_cover_pcnt",
                "away_break_even_odds",
            ]
            odds_df = self.odds_df
            predictions = self.pred_df
            odds_df["winner"] = odds_df.apply(
                lambda row: "home" if row["outcome"] == row["home_team"] else "away",
                axis=1,
            )
            home_merge = pd.merge(
                odds_df[odds_df["winner"] == "home"],
                predictions[pred_home_cols],
                left_on=["home_team", "away_team", "outcome", "point"],
                right_on=[
                    "home_odds_name",
                    "away_odds_name",
                    "home_odds_name",
                    "home_spread",
                ],
                how="left",
                suffixes=["", "_y"],
            )
            away_merge = pd.merge(
                odds_df[odds_df["winner"] == "away"],
                predictions[pred_away_cols],
                left_on=["home_team", "away_team", "outcome", "point"],
                right_on=[
                    "home_odds_name",
                    "away_odds_name",
                    "away_odds_name",
                    "away_spread",
                ],
                how="left",
                suffixes=["", "_y"],
            )
            ev_table = pd.concat([home_merge, away_merge]).reset_index(drop=True)
            for col in ["cover_pcnt", "spread_modeled", "spread", "break_even_odds"]:
                ev_table[col] = ev_table[f"home_{col}"].combine_first(
                    ev_table[f"away_{col}"]
                )
            drop_cols = [
                "winner",
                "home_odds_name",
                "away_odds_name",
                "home_cover_pcnt",
                "home_spread_modeled",
                "home_spread",
                "home_break_even_odds",
                "away_cover_pcnt",
                "away_break_even_odds",
            ]
            ev_table.drop(columns=drop_cols, inplace=True)
            ev_table.dropna(subset=["spread_modeled"], inplace=True)
            ev_table.sort_values(
                by=["game_time", "game_id", "point", "price"],
                ascending=[True, True, False, False],
                inplace=True,
            )
            return ev_table

    def __postprocess_ev_table(self, ev_table):
        ev_table["profit_with_win"] = ev_table["price"].apply(
            lambda x: odds_convert.us_odds_to_profit(x)
        )
        ev_table["expected_value"] = (
            ev_table["profit_with_win"] * ev_table["cover_pcnt"]
        ) + (-1 * (1 - ev_table["cover_pcnt"]))
        ev_table["ideal_bet_pcnt"] = (
            ev_table["expected_value"] / ev_table["profit_with_win"]
        )
        ev_table["ideal_bet_pcnt_diluted"] = (
            ev_table["expected_value"] * self.ev_dilution_factor
        ) / ev_table["profit_with_win"]
        ev_table["ideal_bet_amount"] = np.maximum(
            0, ev_table["ideal_bet_pcnt"] * self.snack_amount
        )
        ev_table["ideal_bet_amount_diluted"] = np.maximum(
            0, ev_table["ideal_bet_pcnt_diluted"] * self.stack_amount
        )
        ev_table["away_spread"].fillna(ev_table["spread"] * -1, inplace=True)
        ev_table["away_spread_modeled"].fillna(
            ev_table["spread_modeled"] * -1.0, inplace=True
        )
        ev_table["away_spread_delta"] = (
            ev_table["away_spread"] - ev_table["away_spread_modeled"]
        )
        ev_table["game"] = (
            ev_table["away_team_short"]
            + " "
            + ev_table["away_spread"].astype(str)
            + " @ "
            + ev_table["home_team_short"]
        )
        ev_table["outcome"] = ev_table["outcome"].map(self.odds_to_tr_team_name_mapper)
        self.ev_table = ev_table
        return ev_table

    def get_ev_table(self, year, week, refresh_predictions, refresh_odds):
        self.get_predictions(
            year=year, week=week, refresh_predictions=refresh_predictions
        )
        self.get_odds()
        self.__merge_to_ev_table()
        self.__postprocess_ev_table()
        return self.ev_table

    def get_best_odds(self, all_books=False):
        if all_books:
            books = self.books
        else:
            books = self.my_books
        ev_table = self.ev_table
        best_bets = ev_table[ev_table["book"].isin(books)]
        best_bets = best_bets.sort_values(
            by=["market", "game_time", "game_id", "expected_value"],
            ascending=[False, True, True, False],
        )
        best_bets = best_bets[self.best_bet_cols]
        best_bets = best_bets.drop_duplicates(subset=["market", "game_id"]).reset_index(
            drop=True
        )
        self.best_bets = best_bets
        return best_bets
