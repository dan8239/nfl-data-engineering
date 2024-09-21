import importlib

import numpy as np
import pandas as pd

from helpers import get_team_id

importlib.reload(get_team_id)


class TeamDataAggregator:
    def __init__(self, stat_db):
        self.stats_db = stat_db
        self.stats_db = self.stats_db.sort_values(by="date", ascending=False)
        self.team_ids = pd.read_csv("reference/team_ids.csv")
        self.skip_columns = ["team", "date"]
        self.dont_aggregate_columns = [
            "team",
            "date",
            "rankings_predictive_rating",
            "rankings_home_rating",
            "rankings_road_rating",
            "rankings_sos_rating",
            "rankings_sos_basic_rating",
            "rankings_luck_rating",
            "rankings_consistency_rating",
        ]
        self.stale_stats_check_columns = [
            "offense_scoring_points_per_game",
            "offense_scoring_yards_per_point",
            "offense_scoring_points_per_play",
            "defense_scoring_ppg",
            "defense_scoring_yards_per_point",
            "defense_scoring_points_per_play",
        ]
        self.sample = None

    def __pull_stats_for_team(self, date, team_box_short_display_name, games_to_sample):
        """
        Pull all rows from the stat_db that correspond to the team ID. Filter the stats for rows prior to the date in question.
        Pull 2x the number of games to keep, then drop duplicates and filter for the most recent x games_to_keep.

        Parameters
        ----------
        date : datetime
            The cutoff date. Only stats prior to this date will be considered.
        team_box_short_display_name : str
            The unique identifier for the team in the stat_db.
        games_to_sample : int
            The number of most recent games to return after filtering.

        Returns
        -------
        DataFrame
            A DataFrame containing the stats for the most recent games_to_sample games for the specified team.
        """
        tr_team_name = get_team_id.get_ids(
            box_short_display_name=team_box_short_display_name
        ).get("tr_team_name")
        team_stats = self.stats_db[
            (self.stats_db["team"] == tr_team_name) & (self.stats_db["date"] < date)
        ]
        sampled_stats = team_stats.head(games_to_sample * 2).drop_duplicates(
            subset=self.stale_stats_check_columns, keep="first"
        )
        final_stats = sampled_stats.head(games_to_sample)
        self.sample = final_stats
        return final_stats

    def __aggregate_team_stats(
        self,
        stat_df,
        aggregation_method="exp_weighted_mean",
        decay_factor=0.9,
        games_to_sample=None,
    ):
        """
        Take the stats dataframe and aggregate over all of the rows. Don't aggregate columns in the self.dont_aggregate_columns list,
        but just keep the most recent value. Make sure that we're ignoring NaNs when doing the aggregation.

        Parameters
        ----------
        stat_df : DataFrame
            The DataFrame containing the stats to be aggregated.
        aggregation_method : str
            The aggregation method to apply (e.g., 'mean', 'exp_weighted_mean').
        decay_factor : float
            The decay factor for the exponential weighting. Should be between 0 and 1.

        Returns
        -------
        DataFrame
            A single-row DataFrame containing the aggregated statistics.
        """

        # Define the exponential weighted mean function
        def exp_weighted_mean(x, games_to_sample=None, min_weight=0):
            n = len(x) if games_to_sample is None else min(games_to_sample, len(x))
            weights = np.array(
                [max(decay_factor ** (n - i - 1), min_weight) for i in range(n)]
            )
            return np.average(x[-n:], weights=weights)

        def inv_log_weighted_mean(x, games_to_sample=None, min_weight=0):
            n = len(x) if games_to_sample is None else min(games_to_sample, len(x))
            weights = np.array(
                [max(1 - (decay_factor ** (n - i)), min_weight) for i in range(n)]
            )
            return np.average(x[-n:], weights=weights)

        if not stat_df.empty:
            agg_columns = [
                col for col in stat_df.columns if col not in self.dont_aggregate_columns
            ]

            if aggregation_method == "exp_weighted_mean":
                aggregated_data = stat_df[agg_columns].apply(
                    lambda x: exp_weighted_mean(x, games_to_sample)
                )
            elif aggregation_method == "inv_log_weighted_mean":
                aggregated_data = stat_df[agg_columns].apply(
                    lambda x: inv_log_weighted_mean(x, games_to_sample)
                )
            else:
                aggregated_data = stat_df[agg_columns].agg(
                    aggregation_method, skipna=True
                )

            for col in self.dont_aggregate_columns:
                if col in stat_df.columns:
                    aggregated_data[col] = stat_df[col].iloc[0]
            df = aggregated_data.to_frame().T
        else:
            df = stat_df
        return df

    def summarize_team(
        self,
        date,
        team_box_short_display_name,
        games_to_sample,
        aggregation_method="inv_log_weighted_mean",
        decay_factor=0.9,
    ):
        """
        Pull all rows from the stat_db that correspond to the team ID. Filter the stats for rows prior to the date in question. Pull 2x the number of games to keep, then drop duplicates and filter for the most recent x games_to_keep. Finally, summarize all the stats over that time period using an average mechanism

        Parameters
        ----------
        date : _type_
            _description_
        team_box_short_display_name : str
            _description_
        games_to_sample : _type_
            _description_
        aggregation_method : _type_
            _description_
        """
        stats_df = self.__pull_stats_for_team(
            date=date,
            team_box_short_display_name=team_box_short_display_name,
            games_to_sample=games_to_sample,
        )
        aggregated_stats = self.__aggregate_team_stats(
            stat_df=stats_df,
            aggregation_method=aggregation_method,
            decay_factor=decay_factor,
            games_to_sample=games_to_sample,
        )
        return aggregated_stats
