import pandas as pd

box_cols = [
    "neutral_site",
    "start_date",
    "type_abbreviation",
    "venue_id",
    "venue_indoor",
    "status_type_completed",
    "status_type_detail",
    "home_id",
    "home_short_display_name",
    "home_abbreviation",
    "home_winner",
    "home_venue_id",
    "home_records",
    "away_id",
    "away_short_display_name",
    "away_abbreviation",
    "away_venue_id",
    "away_records",
    "season_type",
    "week",
    "home_score",
    "away_score",
]
valid_team_names = pd.read_csv("reference/team_ids.csv")[
    "box_short_display_name"
].unique()


def preprocess(df):
    # remove all star BS
    home_mask = df["home_short_display_name"].isin(valid_team_names)
    away_mask = df["away_short_display_name"].isin(valid_team_names)
    df = df[home_mask & away_mask]
    # remove preseason
    df = df[df["season_type"] != 1]
