import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import dotenv
import googlemaps

dotenv.load_dotenv()
key = os.environ.get("GOOGLE_API_KEY")
gmaps = googlemaps.Client(key=key)


def get_lat_lon(row, city_colname="city", state_colname="state"):
    try:
        # Combine city and state into a single address string
        address = f"{row[city_colname]}, {row[state_colname]}"
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            location = geocode_result[0]["geometry"]["location"]
            return (location["lat"], location["lng"])
        else:
            return (None, None)
    except Exception as e:
        print(f"Error geocoding {row['city']}, {row['state']}: {e}")
        return (None, None)


# Function to apply geocoding in parallel
def geocode_dataframe(df, city_colname="city", state_colname="state"):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(get_lat_lon, row, city_colname, state_colname): index
            for index, row in df.iterrows()
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                lat, lon = future.result()
                df.at[index, "latitude"] = lat
                df.at[index, "longitude"] = lon
            except Exception as e:
                print(f"Error processing row {index}: {e}")

    return df
