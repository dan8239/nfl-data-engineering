from datetime import timedelta

import pandas as pd

from weather import weather_client


class WeatherAdder:
    def __init__(self):
        self.weather_client = weather_client.WeatherClient()

    def get_weather_one_game(
        self,
        latitude,
        longitude,
        game_start_datetime,
        future,
    ):
        """
        Get weather for a game given a start time (localized time)

        Parameters
        ----------
        latitude : _type_
            _description_
        longitude : _type_
            _description_
        game_start_datetime : _type_
            _description_
        future: bool
            True if game is in future
        """
        if future:
            weather = self.weather_client.get_weather_forecast(
                latitude=latitude, longitude=longitude, forecast_days=14
            )
        else:
            weather = self.weather_client.get_historical_weather(
                latitude=latitude,
                longitude=longitude,
                start_date=game_start_datetime.stftime("yyyy-mm-dd"),
                end_date=game_start_datetime.stftime("yyyy-mm-dd"),
            )
        return weather

    def add_weather_one_row(self, row, future):
        weather_df = self.get_weather_one_game(
            latitude=row.latitude,
            longitude=row.longitude,
            game_start_datetime=row.local_gametime,
            future=future,
        )
        game_end_time = row.local_gametime + timedelta(hours=3)
        avg_weather = weather_df.loc[
            (weather_df.index >= row.local_gametime)
            & (weather_df.index <= game_end_time)
        ].mean()
        return avg_weather

    def add_weather(self, df, future):
        # Apply the add_weather_one_row to each row of the DataFrame
        weather_data = df.apply(
            lambda row: self.add_weather_one_row(row, future), axis=1
        )
        df_with_weather = pd.concat([df, weather_data], axis=1)
        return df_with_weather
