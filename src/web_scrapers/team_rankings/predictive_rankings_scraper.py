from web_scrapers.team_rankings import team_rankings_scraper


class PredictiveRankingsScraper(team_rankings_scraper.TeamRankingsScraper):
    def __init__(self):
        super().__init__()
        self.base_url = (
            "https://www.teamrankings.com/nfl/ranking/predictive-by-other/?date={}"
        )
        self.cols_to_keep = ["date", "team", "rank", "rating", "hi", "low", "last"]
        self.naming_prefix = "tr_predictive_rankings_"
