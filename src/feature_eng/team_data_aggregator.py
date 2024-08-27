import pandas as pd

from helpers import get_team_id


class TeamDataAggregator:
    def __init__(self, stat_db):
        self.stats_db = stat_db
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
        self.sample = None

    def __pull_stats_for_team(self, date, team_merge_id, games_to_sample):
        """
        Pull all rows from the stat_db that correspond to the team ID. Filter the stats for rows prior to the date in question.
        Pull 2x the number of games to keep, then drop duplicates and filter for the most recent x games_to_keep.

        Parameters
        ----------
        date : datetime
            The cutoff date. Only stats prior to this date will be considered.
        team_merge_id : str or int
            The unique identifier for the team in the stat_db.
        games_to_sample : int
            The number of most recent games to return after filtering.

        Returns
        -------
        DataFrame
            A DataFrame containing the stats for the most recent games_to_sample games for the specified team.
        """
        tr_team_name = get_team_id.get_ids(merge_team_id=team_merge_id).get(
            "tr_team_name"
        )
        team_stats = self.stats_db[
            (self.stats_db["team"] == tr_team_name) & (self.stats_db["date"] < date)
        ]
        team_stats = team_stats.sort_values(by="date", ascending=False)
        dup_cols = team_stats.drop(columns="date").columns
        sampled_stats = team_stats.head(games_to_sample * 2).drop_duplicates(
            subset=dup_cols, keep="first"
        )
        final_stats = sampled_stats.head(games_to_sample)
        self.sample = final_stats
        return final_stats

    def __aggregate_team_stats(self, stat_df, aggregation_method="mean"):
        """
        Take the stats dataframe and aggregate over all of the rows. Don't aggregate columns in the self.dont_aggregate_columns list,
        but just keep the most recent value. Make sure that we're ignoring NaNs when doing the aggregation.

        Parameters
        ----------
        stat_df : DataFrame
            The DataFrame containing the stats to be aggregated.
        aggregation_method : str
            The aggregation method to apply (e.g., 'mean').

        Returns
        -------
        DataFrame
            A single-row DataFrame containing the aggregated statistics.
        """
        agg_columns = [
            col for col in stat_df.columns if col not in self.dont_aggregate_columns
        ]
        aggregated_data = stat_df[agg_columns].agg(aggregation_method, skipna=True)
        for col in self.dont_aggregate_columns:
            if col in stat_df.columns:
                aggregated_data[col] = stat_df[col].iloc[0]

        return aggregated_data.to_frame().T

    def summarize_team(
        self, date, team_merge_id, games_to_sample, aggregation_method="mean"
    ):
        """
        Pull all rows from the stat_db that correspond to the team ID. Filter the stats for rows prior to the date in question. Pull 2x the number of games to keep, then drop duplicates and filter for the most recent x games_to_keep. Finally, summarize all the stats over that time period using an average mechanism

        Parameters
        ----------
        date : _type_
            _description_
        team_merge_id : _type_
            _description_
        games_to_sample : _type_
            _description_
        aggregation_method : _type_
            _description_
        """
        stats_df = self.__pull_stats_for_team(
            date=date, team_merge_id=team_merge_id, games_to_sample=games_to_sample
        )
        aggregated_stats = self.__aggregate_team_stats(
            stat_df=stats_df, aggregation_method=aggregation_method
        )
        return aggregated_stats
