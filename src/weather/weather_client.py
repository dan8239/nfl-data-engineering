import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry


class WeatherClient:
    def __init__(self):
        """
        Initializes the WeatherClient with a cached session and retry mechanism.

        A CachedSession is used to cache API responses, and the retry mechanism
        ensures that failed requests are retried up to 5 times with exponential backoff.

        Attributes
        ----------
        client : openmeteo_requests.Client
            Client for making API requests to Open Meteo.
        historical_url : str
            URL endpoint for historical weather data.
        """
        cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.client = openmeteo_requests.Client(session=retry_session)
        self.historical_url = "https://archive-api.open-meteo.com/v1/archive"
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"

    def get_historical_weather(
        self,
        latitude,
        longitude,
        start_date,
        end_date,
        param_list=[
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "rain",
            "snowfall",
            "snow_depth",
            "cloud_cover",
            "wind_speed_10m",
            "wind_gusts_10m",
        ],
        temperature_unit="fahrenheit",
        wind_speed_unit="mph",
        precipitation_unit="inch",
    ):
        """
        Retrieves historical hourly weather data for a specified location and date range.

        This method fetches historical weather data such as temperature, humidity, wind speed,
        and more from the Open-Meteo API for a given location and date range, with the ability
        to specify different units for temperature, wind speed, and precipitation. The resulting
        data is returned as a pandas DataFrame with a localized datetime index.

        Parameters
        ----------
        latitude : float
            The latitude of the location for which to retrieve weather data.
        longitude : float
            The longitude of the location for which to retrieve weather data.
        start_date : str
            The start date for the weather data retrieval, in 'YYYY-MM-DD' format.
        end_date : str
            The end date for the weather data retrieval, in 'YYYY-MM-DD' format.
        param_list : list of str, optional
            A list of weather parameters to retrieve (default is ["temperature_2m",
            "relative_humidity_2m", "apparent_temperature", "rain", "snowfall",
            "snow_depth", "cloud_cover", "wind_speed_10m", "wind_gusts_10m"]).
        temperature_unit : str, optional
            The unit for temperature values, either "fahrenheit" or "celsius" (default is "fahrenheit").
        wind_speed_unit : str, optional
            The unit for wind speed values, either "mph" or "km/h" (default is "mph").
        precipitation_unit : str, optional
            The unit for precipitation values, either "inch" or "mm" (default is "inch").

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the historical weather data. The index of the DataFrame
            is a datetime index localized to the timezone of the location. The columns correspond
            to the requested weather parameters.

            Example columns include:
            - 'temperature_2m'
            - 'relative_humidity_2m'
            - 'apparent_temperature'
            - 'rain'
            - 'snowfall'
            - 'snow_depth'
            - 'cloud_cover'
            - 'wind_speed_10m'
            - 'wind_gusts_10m'

        Notes
        -----
        - The datetime index is initially generated in UTC and then converted to the timezone of the location,
        using the timezone information returned by the Open-Meteo API.
        - Ensure that the requested parameters in `param_list` are valid according to the Open-Meteo API documentation.

        Examples
        --------
        >>> weather_client = WeatherClient()
        >>> df = weather_client.get_historical_weather(
                latitude=52.54,
                longitude=13.41,
                start_date="2022-01-01",
                end_date="2022-01-07"
            )
        >>> print(df.head())
                            temperature_2m  relative_humidity_2m  apparent_temperature  rain  snowfall  ...
        2022-01-01 00:00:00             34.1                 88.2                 30.4   0.0      0.0
        2022-01-01 01:00:00             33.8                 87.5                 30.2   0.0      0.0
        2022-01-01 02:00:00             33.4                 86.9                 30.1   0.0      0.0
        2022-01-01 03:00:00             33.2                 85.3                 29.8   0.0      0.0
        2022-01-01 04:00:00             33.0                 84.1                 29.6   0.0      0.0
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": param_list,
            "temperature_unit": temperature_unit,
            "wind_speed_unit": wind_speed_unit,
            "precipitation_unit": precipitation_unit,
        }
        responses = self.client.weather_api(self.historical_url, params=params)
        response = responses[0]
        timezone_name = response.Timezone()
        time_index_utc = pd.date_range(
            start=pd.to_datetime(response.Hourly().Time(), unit="s", utc=True),
            end=pd.to_datetime(response.Hourly().TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=response.Hourly().Interval()),
            inclusive="left",
        )
        time_index_local = time_index_utc.tz_convert(timezone_name)
        hourly_data = {
            "temperature_2m": response.Hourly().Variables(0).ValuesAsNumpy(),
            "relative_humidity_2m": response.Hourly().Variables(1).ValuesAsNumpy(),
            "apparent_temperature": response.Hourly().Variables(2).ValuesAsNumpy(),
            "rain": response.Hourly().Variables(3).ValuesAsNumpy(),
            "snowfall": response.Hourly().Variables(4).ValuesAsNumpy(),
            "snow_depth": response.Hourly().Variables(5).ValuesAsNumpy(),
            "cloud_cover": response.Hourly().Variables(6).ValuesAsNumpy(),
            "wind_speed_10m": response.Hourly().Variables(7).ValuesAsNumpy(),
            "wind_gusts_10m": response.Hourly().Variables(8).ValuesAsNumpy(),
        }
        df = pd.DataFrame(hourly_data, index=time_index_local)
        return df

    def get_weather_forecast(
        self,
        latitude,
        longitude,
        forecast_days=14,
        param_list=[
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation_probability",
            "precipitation",
            "rain",
            "showers",
            "snowfall",
            "snow_depth",
            "cloud_cover",
            "wind_speed_10m",
            "wind_gusts_10m",
        ],
        temperature_unit="fahrenheit",
        wind_speed_unit="mph",
        precipitation_unit="inch",
    ):
        """
        Retrieve forecast weather data for a specific location.

        Parameters
        ----------
        latitude : float
            Latitude of the location.
        longitude : float
            Longitude of the location.
        forecast_days : int, optional
            Number of days to forecast, starting from today. Default is 7.
        param_list : list of str, optional
            List of parameters to retrieve, by default it includes
            temperature, humidity, wind speed, etc.
        temperature_unit : str, optional
            Unit for temperature, by default "fahrenheit".
        wind_speed_unit : str, optional
            Unit for wind speed, by default "mph".
        precipitation_unit : str, optional
            Unit for precipitation, by default "inch".

        Returns
        -------
        pd.DataFrame
            DataFrame with the weather forecast, including a datetime index and columns for each variable.

        Example
        -------
        >>> wc = WeatherClient()
        >>> df = wc.get_forecast_weather(latitude=52.52, longitude=13.41, forecast_days=5)
        >>> print(df.head())
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": param_list,
            "temperature_unit": temperature_unit,
            "wind_speed_unit": wind_speed_unit,
            "precipitation_unit": precipitation_unit,
            "forecast_days": forecast_days,
        }
        responses = self.client.weather_api(self.forecast_url, params=params)
        response = responses[0]
        timezone = response.Timezone()
        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s")
                .tz_localize("UTC")
                .tz_convert(timezone),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s")
                .tz_localize("UTC")
                .tz_convert(timezone),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }
        for i, param in enumerate(param_list):
            hourly_data[param] = hourly.Variables(i).ValuesAsNumpy()
        hourly_dataframe = pd.DataFrame(data=hourly_data)
        return hourly_dataframe


if __name__ == "__main__":
    wc = WeatherClient()
    lat = 40.0
    lon = -87.0
    weather_df = wc.get_historical_weather(
        latitude=lat, longitude=-lon, start_date="2023-09-13", end_date="2023-09-13"
    )
    print(weather_df)
    forecast_df = wc.get_weather_forecast(latitude=lat, longitude=lon, forecast_days=7)
    print(forecast_df)
