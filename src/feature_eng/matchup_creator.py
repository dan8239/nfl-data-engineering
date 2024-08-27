import pandas as pd

from feature_eng import calc_differential, team_data_aggregator


class MatchupCreator:
    def __init__(self, stats_db, games_to_sample=8, aggregation_method="mean"):
        self.tda = team_data_aggregator.TeamDataAggregator(stat_db=stats_db)
        self.games_to_sample = games_to_sample
        self.aggregation_method = aggregation_method
        self.home_stats = None
        self.road_stats = None
        self.diff_stats = None

    def create_matchup(self, home_team_merge_id, road_team_merge_id, date):
        """
        create a matchup given a date and two team ID's. This includes all the features that are needed for a model to learn on

        Parameters
        ----------
        home_team_merge_id : int
            ID of the team that is playing at home (merge_id from ref table)
        road_team_merge_id : int
            ID of the team that is playing on the road (merge_id from ref table)
        date: str
            date the game is being played

        returns
        ----------
        pd.DataFrame
            dataframe w/ all the data collected and feature engineered
        """
        print("creating matchup")
        home_perf_summary_df = self.tda.summarize_team(
            date=date,
            team_merge_id=home_team_merge_id,
            games_to_sample=self.games_to_sample,
            aggregation_method=self.aggregation_method,
        )
        self.home_stats = home_perf_summary_df
        road_perf_summary_df = self.tda.summarize_team(
            date=date,
            team_merge_id=road_team_merge_id,
            games_to_sample=self.games_to_sample,
            aggregation_method=self.aggregation_method,
        )
        self.road_stats = road_perf_summary_df
        perf_diff_df = calc_differential.calc_performance_differential(
            df1=home_perf_summary_df, df2=road_perf_summary_df
        )
        self.diff_stats = perf_diff_df
        home_perf_summary_df = home_perf_summary_df.add_prefix("home_")
        road_perf_summary_df = road_perf_summary_df.add_prefix("road_")
        combined_matchup_df = pd.concat(
            [home_perf_summary_df, road_perf_summary_df, perf_diff_df], axis=1
        )
        return combined_matchup_df
