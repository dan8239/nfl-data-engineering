import ssl

import pandas as pd


class TeamRankingsScraper:
    def __init__(self):
        ssl._create_default_https_context = ssl._create_unverified_context
        self.base_url = None
        self.cols_to_keep = None
        self.naming_prefix = None

    def _strip_team_names(self, df):
        """

        Strip the " (W-L-T)" from team names

        Args:
            df (_type_): _description_

        Returns:
            _type_: _description_
        """
        df["Team"] = df["Team"].str.replace(r"\s\(.*\)", "", regex=True)
        return df

    def _add_date_to_df(self, df, date):
        df["date"] = date
        return df

    def _col_names_to_lower_case(self, df):
        df.columns = map(str.lower, df.columns)
        return df

    def _add_prefixes_to_col_names(self, df, prefix, skip_cols):
        df = df.rename(
            columns={
                col: prefix + col if col not in skip_cols else col for col in df.columns
            }
        )
        return df

    def get_table(self, date):
        """
        get a table from a teamrankings site and format it

        Returns:
            pd.DataFrame: cleansed dataframe
        """
        url = self.base_url.format(date)
        tables = pd.read_html(url)
        df = tables[0]
        df = self._add_date_to_df(df, date)
        df = self._strip_team_names(df)
        df = self._col_names_to_lower_case(df)
        df = df[self.cols_to_keep]
        df = self._add_prefixes_to_col_names(
            df=df, prefix=self.naming_prefix, skip_cols=["date", "team"]
        )
        return df
