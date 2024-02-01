import os
import ssl

import pandas as pd


class TeamRankingsScraper:
    def __init__(self):
        print()
        print(os.getcwd())
        ssl._create_default_https_context = ssl._create_unverified_context
        self.url_df = pd.read_excel(
            "web_scrapers/team_rankings/urls_team_rankings.xlsx"
        )

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

    def _split_win_loss(self, df, columns):
        for col in columns:
            df[[f"{col}_wins", f"{col}_losses"]] = df[col].str.split("-", expand=True)
            df[[f"{col}_wins", f"{col}_losses"]] = df[
                [f"{col}_wins", f"{col}_losses"]
            ].astype(int)
            df[f"{col}_games_played"] = df[f"{col}_wins"] + df[f"{col}_losses"]
            df.drop(columns=col, inplace=True)
        return df

    def _col_names_to_lower_case(self, df):
        df.columns = map(str.lower, df.columns)
        return df

    def _add_prefixes_to_col_names(self, df, prefix, cols_to_process):
        df = df.rename(
            columns={
                col: prefix + col if col in cols_to_process else col
                for col in df.columns
            }
        )
        return df

    def _drop_spaces_in_col_names(self, df):
        df.columns = df.columns.str.replace(" ", "")
        return df

    def __get_table(
        self, category, table_name, base_url, cols_to_keep, record_cols, date
    ):
        """
        Get a single table from a team rankings site and process it into a dataframe

        Args:
            category (str): category of table (off, def, st, etc.)
            table_name (str): table name (predictive, yards, tds, etc.)
            base_url (str): url of the table
            cols_to_keep (list(str)): which columns to keep in the table
            date (str): date value to get table data from (yyyy-mm-dd)
            record_cols (list(str)): cols w/ records (3-1) that need s  ome processing

        Returns:
            pd.DataFrame: formatted table w/ date and colnames adjusted
        """
        url = f"{base_url}?date={date}"
        print(f"getting {url}")
        tables = pd.read_html(url)
        df = tables[0]
        df = self._strip_team_names(df)
        df = df[["Team"] + cols_to_keep]
        df = self._split_win_loss(df, columns=record_cols)
        df = self._col_names_to_lower_case(df)
        df = self._drop_spaces_in_col_names(df)
        df = self._add_prefixes_to_col_names(
            df=df,
            prefix=f"{category}_{table_name}_",
            cols_to_process=df.drop(columns="team").columns,
        )
        return df

    def get_all_tables_for_date(self, date):
        """_summary_

        Args:
            date (_type_): _description_
        """
        all_stats_df = pd.DataFrame()
        for i, row in self.url_df.iterrows():
            keep_cols = [
                element.strip()
                for element in row.cols_to_keep.split(",")
                if element.strip()
            ]
            record_cols = [
                element.strip()
                for element in row.record_cols.split(",")
                if element.strip()
            ]
            df = self.__get_table(
                category=row.category,
                table_name=row.table_name,
                base_url=row.base_url,
                cols_to_keep=keep_cols,
                record_cols=record_cols,
                date=date,
            )
            if all_stats_df.empty:
                all_stats_df = df
            else:
                all_stats_df = pd.merge(
                    left=all_stats_df, right=df, how="left", on="team"
                )
        all_stats_df = self._add_date_to_df(df, date)
        return all_stats_df
