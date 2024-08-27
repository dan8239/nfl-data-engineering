import pandas as pd

performance_mapping_df = pd.read_csv("reference/performance_differential_colums.csv")


def _calc_differential(df1, df2, mapping_df):
    """
    Creates a DataFrame representing the difference between values in df1 and df2,
    with columns from df2 subtracted from corresponding columns in df1.
    The column names are mapped using the mapping_df DataFrame.

    Parameters
    ----------
    df1 : pd.DataFrame
        The first DataFrame from which values will be subtracted.
    df2 : pd.DataFrame
        The second DataFrame whose values will be subtracted from df1.
    mapping_df : pd.DataFrame
        A DataFrame that contains the mapping of columns from df1 to df2.
        It should have two columns: 'home_stat' and 'road_stat', where
        'home_stat' refers to the column in df1 and 'road_stat' refers to the
        corresponding column in df2.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the differences with column names from df1.
    """
    diff_df = pd.DataFrame()

    for _, row in mapping_df.iterrows():
        df1_col = row["home_stat"]
        df2_col = row["road_stat"]

        # Check if both columns exist in their respective DataFrames
        if df1_col in df1.columns and df2_col in df2.columns:
            diff_df[df1_col + "_matchup_differential"] = df1[df1_col] - df2[df2_col]
        else:
            raise KeyError(
                f"Column '{df1_col}' or '{df2_col}' not found in the respective DataFrames"
            )

    return diff_df


def calc_performance_differential(df1, df2):
    """
    Creates a DataFrame representing the difference between values in df1 and df2,
    with columns from df2 subtracted from corresponding columns in df1.
    The column names are mapped using the performance DataFrame columns.

    Parameters
    ----------
    df1 : pd.DataFrame
        The first DataFrame from which values will be subtracted.
    df2 : pd.DataFrame
        The second DataFrame whose values will be subtracted from df1.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the differences with column names from df1.
    """
    return _calc_differential(df1=df1, df2=df2, mapping_df=performance_mapping_df)
