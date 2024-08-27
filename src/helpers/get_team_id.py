import pandas as pd

team_id_df = pd.read_csv("reference/team_ids.csv")


def get_ids(**kwargs):
    """
    Get a dict of all the different IDs from an input team ID.
    Must pass one of the following as a keyword argument:
    - merge_team_id
    - tr_team_name
    - box_short_display_name

    Returns:
    - A dictionary with all relevant IDs for the team.
    """

    if "merge_team_id" in kwargs:
        team_row = team_id_df[team_id_df["merge_team_id"] == kwargs["merge_team_id"]]
    elif "tr_team_name" in kwargs:
        team_row = team_id_df[team_id_df["tr_team_name"] == kwargs["tr_team_name"]]
    elif "box_short_display_name" in kwargs:
        team_row = team_id_df[
            team_id_df["box_short_display_name"] == kwargs["box_short_display_name"]
        ]
    else:
        raise ValueError(
            "Must pass one of merge_team_id, tr_team_name, or box_short_display_name as a keyword argument"
        )

    if team_row.empty:
        raise ValueError("No matching team found with the provided ID")

    # Convert the row to a dictionary and return
    return team_row.to_dict(orient="records")[0]
