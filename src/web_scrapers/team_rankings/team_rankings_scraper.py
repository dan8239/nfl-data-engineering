import os
import ssl
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta


class TeamRankingsScraper:
    def __init__(self):
        print()
        print(os.getcwd())
        ssl._create_default_https_context = ssl._create_unverified_context
        url_df = pd.read_excel("web_scrapers/team_rankings/urls_team_rankings.xlsx")
        self.url_df = url_df.fillna("")

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
        """add a column representing the date in the 2nd column (after team)

        Args:
            df (pd.DataFrame): df to transform
            date (str): YYYY-MM-DD

        Returns:
            pd.DataFrame: transformed df
        """
        df.insert(1, "date", date)
        return df

    def _split_win_loss(self, df, columns):
        """change values in W-L columns from 1-1 to three columns, wins, losses, and total games

        Args:
            df (pd.DataFrame): dataframe to transform
            columns (list(str)): columns to split

        Returns:
            pd.DataFrame: transformed df
        """
        for col in columns:
            df[[f"{col}_wins", f"{col}_losses"]] = df[col].str.split("-", expand=True)
            df[[f"{col}_wins", f"{col}_losses"]] = df[
                [f"{col}_wins", f"{col}_losses"]
            ].astype(int)
            df[f"{col}_games_played"] = df[f"{col}_wins"] + df[f"{col}_losses"]
            df.drop(columns=col, inplace=True)
        return df

    def _col_names_to_lower_case(self, df):
        """change formatting to all lower case on column names

        Args:
            df (pd.DataFrame): dataframe to transform

        Returns:
            pd.DataFrame: transformed df
        """
        df.columns = map(str.lower, df.columns)
        return df

    def _add_prefixes_to_col_names(self, df, prefix, cols_to_process):
        """Add prefix to all column names given on a dataframe

        Args:
            df (pd.DataFrame): dataframe to transform
            prefix (str): prefix to add to column names
            cols_to_process (list(str)): columns to change names of

        Returns:
            pd.DataFrame: transformed df
        """
        df = df.rename(
            columns={
                col: prefix + col if col in cols_to_process else col
                for col in df.columns
            }
        )
        return df

    def _drop_spaces_in_col_names(self, df):
        """remove spaces in naming convention

        Args:
            df (pd.DataFrame): dataframe to transform

        Returns:
            pd.DataFrame: transformed dataframe
        """
        df.columns = df.columns.str.replace(" ", "")
        return df

    def _replace_year_placeholders(self, df, date, cols):
        """replace the f-string year and last year brackets on the url df

        Args:
            df (pd.DataFrame): dataframe to transform
            date (str): "YYYY-MM-DD"
            cols (list(str)): columns to replace

        Returns:
            _type_: _description_
        """
        this_year_datetime = datetime.strptime(date, "%Y-%m-%d")
        last_year_datetime = this_year_datetime - relativedelta(years=1)
        replace_dct = {
            "year": str(this_year_datetime.year),
            "last_year": str(last_year_datetime.year),
        }

        def replace_placeholders(text):
            return text.format(**replace_dct)

        for col in cols:
            # Apply the replacement function to the DataFrame column
            df[col] = df[col].apply(replace_placeholders)
        return df

    def _replace_null_markers(self, df):
        string_columns = df.select_dtypes(include="object").columns
        df[string_columns] = df[string_columns].replace("--", "", regex=True)
        return df

    def _replace_percentage_strings(self, df):
        def replace_percentage_strings(value):
            if isinstance(value, str) and "%" in value:
                return float(value.rstrip("%")) / 100.0
            else:
                return value

        df = df.applymap(replace_percentage_strings)
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
        """get all the table data for a single date

        Args:
            date (str): YYYY-MM-DD

        Returns:
            pd.DataFrame: data for all teams on one date in DF
        """
        all_stats_df = pd.DataFrame()
        url_df = self._replace_year_placeholders(
            df=self.url_df, date=date, cols=["cols_to_keep"]
        )
        for i, row in url_df.iterrows():
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
                print(all_stats_df.shape)
        all_stats_df = self._add_date_to_df(all_stats_df, date)
        all_stats_df = self._replace_null_markers(all_stats_df)
        all_stats_df = self._replace_percentage_strings(all_stats_df)
        print(all_stats_df.shape)
        all_stats_df.to_excel("../output/tr_all_stats.xlsx", index=False)
        return all_stats_df
