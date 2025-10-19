import os
import random
import ssl
import time
from datetime import datetime

import pandas as pd


class TeamRankingsScraper:
    def __init__(self):
        print()
        print(os.getcwd())
        ssl._create_default_https_context = ssl._create_unverified_context
        url_df = pd.read_excel("data_clients/team_rankings/urls_team_rankings.xlsx")
        self.url_df = url_df.fillna("")
        self.stats_df = None
        self.stats_df_path = "../data/raw/tr_stats_short.xlsx"

    def __strip_team_names(self, df):
        """

        Strip the " (W-L-T)" from team names

        Args:
            df (_type_): _description_

        Returns:
            _type_: _description_
        """
        df["Team"] = df["Team"].str.replace(r"\s\(.*\)", "", regex=True)
        return df

    def __add_date_to_df(self, df, date):
        """add a column representing the date in the 2nd column (after team)

        Args:
            df (pd.DataFrame): df to transform
            date (str): YYYY-MM-DD

        Returns:
            pd.DataFrame: transformed df
        """
        date_str = datetime.strftime(date, "%Y-%m-%d")
        df.insert(1, "date", date_str)
        return df

    def __split_win_loss(self, df, columns):
        """change values in W-L columns from 1-1 to three columns, wins, losses, and total games

        Args:
            df (pd.DataFrame): dataframe to transform
            columns (list(str)): columns to split

        Returns:
            pd.DataFrame: transformed df
        """
        for col in columns:
            # Use regular expression to match any number of digits separated by hyphen
            split_result = df[col].str.extract(r"(\d+)-(\d+)(?:-(\d+))?")
            if split_result is not None:
                if split_result.shape[1] == 2:
                    split_result.columns = [f"{col}_wins", f"{col}_losses"]
                    split_result[f"{col}_ties"] = 0
                elif split_result.shape[1] == 3:
                    split_result.columns = [
                        f"{col}_wins",
                        f"{col}_losses",
                        f"{col}_ties",
                    ]
                else:
                    raise ValueError(f"Unexpected format in column '{col}'")
                split_result = split_result.infer_objects(copy=False).fillna(0)
                split_result = split_result.astype(int)
                df[f"{col}_games_played"] = (
                    split_result[f"{col}_wins"]
                    + split_result[f"{col}_losses"]
                    + split_result[f"{col}_ties"]
                )
                df = pd.concat([df, split_result], axis=1)
                df.drop(columns=col, inplace=True)
        return df

    def __col_names_to_lower_case(self, df):
        """change formatting to all lower case on column names

        Args:
            df (pd.DataFrame): dataframe to transform

        Returns:
            pd.DataFrame: transformed df
        """
        df.columns = map(str.lower, df.columns)
        return df

    def __add_prefixes_to_col_names(self, df, prefix, cols_to_process):
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

    def __drop_spaces_in_col_names(self, df):
        """remove spaces in naming convention

        Args:
            df (pd.DataFrame): dataframe to transform

        Returns:
            pd.DataFrame: transformed dataframe
        """
        df.columns = df.columns.str.replace(" ", "")
        return df

    def __replace_weird_symbols(self, df):
        string_columns = df.select_dtypes(include="object").columns
        df[string_columns] = df[string_columns].replace("--", "", regex=True)
        df[string_columns] = df[string_columns].replace("\\+", "", regex=True)
        return df

    def __replace_percentage_strings(self, df):
        def replace_percentage_strings(value):
            if isinstance(value, str) and "%" in value:
                return float(value.rstrip("%")) / 100.0
            else:
                return value

        df = df.map(replace_percentage_strings)
        return df

    def __rename_year_cols(self, df):
        years_list = [str(year) for year in range(2000, 2101)]
        year_cols = [col for col in df.columns if col in years_list]
        if year_cols:
            df.rename(
                columns={year_cols[0]: "this_yr", year_cols[1]: "last_yr"}, inplace=True
            )
        return df

    def _get_table(self, base_url, date):
        """
        Get a single table from a team rankings site

        Args:
            base_url (str): url of the table
            date (datetime): date value to get table data from (yyyy-mm-dd)

        Returns:
            pd.DataFrame: table in df form
        """
        date_str = datetime.strftime(date, "%Y-%m-%d")
        url = f"{base_url}?date={date_str}"
        print(f"getting {url}")
        random_float = random.uniform(0, 2)
        time.sleep(random_float)
        tables = pd.read_html(url)
        df = tables[0]
        return df

    def _postprocess_df(self, df, record_cols, category, table_name):
        """process the dataframe

        Args:
            df (pd.DataFrame): dataframe to postprocess
            record_cols (list(str)): cols w/ records (3-1) that need some processing
            category (str): category of table (off, def, st, etc.)
            table_name (str): table name (predictive, yards, tds, etc.)

        Returns:
            dataframe: processed df
        """
        print("postprocessing")
        df = self.__strip_team_names(df)
        df = self.__split_win_loss(df, columns=record_cols)
        df = self.__col_names_to_lower_case(df)
        df = self.__drop_spaces_in_col_names(df)
        df = self.__rename_year_cols(df)
        df = self.__add_prefixes_to_col_names(
            df=df,
            prefix=f"{category}_{table_name}_",
            cols_to_process=df.drop(columns="team").columns,
        )
        return df

    def __obj_cols_to_str(self, df):
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str)
        return df

    def get_all_tables_for_date(self, date):
        """get all the table data for a single date

        Args:
            date (datetime):

        Returns:
            pd.DataFrame: data for all teams on one date in DF
        """
        all_stats_df = pd.DataFrame()
        for i, row in self.url_df.iterrows():
            record_cols = [
                element.strip()
                for element in row.record_cols.split(",")
                if element.strip()
            ]
            df = self._get_table(
                base_url=row.base_url,
                date=date,
            )
            df = self._postprocess_df(
                df=df,
                record_cols=record_cols,
                category=row.category,
                table_name=row.table_name,
            )
            if all_stats_df.empty:
                all_stats_df = df
            else:
                all_stats_df = pd.merge(
                    left=all_stats_df, right=df, how="left", on="team"
                )
                print(all_stats_df.shape)
        all_stats_df = self.__add_date_to_df(all_stats_df, date)
        all_stats_df = self.__replace_weird_symbols(all_stats_df)
        all_stats_df = self.__replace_percentage_strings(all_stats_df)
        all_stats_df = self.__obj_cols_to_str(all_stats_df)
        print(all_stats_df.shape)
        return all_stats_df

    def append_date_to_database(self, date):
        if self.stats_df is None:
            print("reading stats db")
            stats_df = pd.read_excel(self.stats_df_path)
            print(f"existing stats db shape: {stats_df.shape}")
            self.stats_df = stats_df
        df = self.get_all_tables_for_date(date)
        stats_df = pd.concat([stats_df, df], ignore_index=True)
        stats_df.to_excel(self.stats_df_path, index=False)
