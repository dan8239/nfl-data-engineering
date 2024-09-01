from datetime import datetime

import pandas as pd
import pytz
from timezonefinder import TimezoneFinder

tz_finder = TimezoneFinder()


def get_time_zone_and_offset(lat_series, lon_series):
    def get_offset(lat, lon):
        if pd.isna(lat) or pd.isna(lon):
            return (None, None)
        tz_str = tz_finder.timezone_at(lat=lat, lng=lon)
        if tz_str:
            tz = pytz.timezone(tz_str)
            now = datetime.utcnow()  # Create a naive UTC datetime
            local_time = tz.localize(
                datetime.combine(now.date(), datetime.min.time())
            )  # Localize to the timezone
            utc_offset = (
                local_time.utcoffset().total_seconds() / 3600
            )  # Convert to hours
            return (tz_str, utc_offset)
        return (None, None)

    # Apply function to each pair of lat/lon
    offsets = [get_offset(lat, lon) for lat, lon in zip(lat_series, lon_series)]
    time_zone_series, utc_offset_series = zip(
        *offsets
    )  # Unpack into two separate lists
    return pd.Series(time_zone_series, index=lat_series.index), pd.Series(
        utc_offset_series, index=lat_series.index
    )
