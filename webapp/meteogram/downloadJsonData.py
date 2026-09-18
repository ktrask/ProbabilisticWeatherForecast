from datetime import date, timedelta, datetime
import time, json, sys
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError, GeocoderTimedOut
import requests
import getopt
import numpy as np
import pandas as pd

def calculate_percentiles(df, column_string="temperature_2m"):
    # Select only the 51 temperature columns
    temp_cols = [f'{column_string}_member{i}' for i in range(51)]
    temps = df[temp_cols].to_numpy()

    # Compute the required percentiles
    percentiles = [0, 10, 25, 50, 75, 90, 100]  # min, 10%, 25%, median, 75%, 90%, max
    percentile_values = np.percentile(temps, percentiles, axis=1).T

    # Create a DataFrame with the percentile results
    percentile_df = pd.DataFrame(
        percentile_values,
        columns=['min', 'ten', 'twenty_five', 'median', 'seventy_five', 'ninety', 'max']
    )

    # Concatenate with the date or any other columns you want to retain
    result = pd.concat([df[['date']], percentile_df], axis=1)

    print(result.head())
    return(result)


STEP_INTERVAL_HOURS = 6


def accumulate_over_steps(df, column_string="precipitation", step_interval=STEP_INTERVAL_HOURS):
    """Sum each member's hourly totals into step_interval-hour buckets.

    Open-Meteo reports precipitation as the millimetres that fell during each
    hour. Sampling every nth row - which is what create_dictionary does, and is
    correct for instantaneous variables like temperature - would silently throw
    away the rain that fell in the hours between the samples.

    Bucket k covers [k*step_interval, (k+1)*step_interval) hours from the start,
    so it lines up with the instantaneous sample taken at its first hour. A short
    trailing bucket is kept, which keeps the bucket count equal to the number of
    samples the other variables produce.

    The summing has to happen per member, before percentiles are taken: the 90th
    percentile of the 6-hour totals is not the sum of the hourly 90th percentiles.
    """
    member_cols = [f'{column_string}_member{i}' for i in range(51)]
    buckets = df.index // step_interval
    summed = df.groupby(buckets)[member_cols].sum()
    summed.insert(0, "date", df.groupby(buckets)["date"].first())
    return summed.reset_index(drop=True)


def create_dictionary(df, name="2t", step_interval=1):
    n_steps = len(df)
    step_size_hours = int((df['date'].iloc[1] - df['date'].iloc[0]).total_seconds() / 3600)

    # Select every nth row using the step_interval
    selected_rows = df.iloc[::step_interval]
    selected_indices = range(0, n_steps, step_interval)

    # Generate the 'steps' list dynamically based on the index positions selected
    steps = [str(i * step_size_hours) for i in selected_indices]

    first_date = pd.to_datetime(df['date'].iloc[0])

    output = {
        name: {
            "max": selected_rows['max'].tolist(),
            "median": selected_rows['median'].tolist(),
            "min": selected_rows['min'].tolist(),
            "ninety": selected_rows['ninety'].tolist(),
            "seventy_five": selected_rows['seventy_five'].tolist(),
            "ten": selected_rows['ten'].tolist(),
            "twenty_five": selected_rows['twenty_five'].tolist(),
            "steps": steps,
        },
        "date": first_date.strftime('%Y%m%d'),
        "time": first_date.strftime('%H%M')
    }

    return output

#requests has no default timeout, so every call below sets one explicitly: a
#hung third party would otherwise tie up a worker indefinitely.
ELEVATION_TIMEOUT = 10
GEOCODE_TIMEOUT = 10
OPEN_METEO_TIMEOUT = 30

UNKNOWN_ELEVATION = -999


class LocationNotFound(ValueError):
    """A place name could not be resolved to coordinates.

    Raised rather than returning None so callers cannot accidentally carry a
    missing coordinate into a forecast request; the web layer turns it into a
    400 with the name the user typed.
    """


def getElevation(latitude, longitute):
    """Elevation in metres from open-elevation.com, or None if it cannot say.

    Only used by the command line path. Open-Meteo reports the elevation of the
    grid cell it actually used, which getData() hands back through `metadata`,
    so the web path does not need this second service at all.
    """
    baseUrl = "https://api.open-elevation.com"
    try:
        response = requests.get(
            f"{baseUrl}/api/v1/lookup?locations={latitude},{longitute}",
            timeout=ELEVATION_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()["results"][0]["elevation"]
    except (requests.RequestException, ValueError, KeyError, IndexError) as exc:
        #Elevation is decoration on the plot title; never fail a forecast for it.
        print(f"elevation lookup failed ({type(exc).__name__}: {exc}), continuing without it")
        return None


def geocodeLocation(name):
    """Resolve a place name to (latitude, longitude), or raise LocationNotFound."""
    geolocator = Nominatim(user_agent="ESOWC-Meteogram-2018", timeout=GEOCODE_TIMEOUT)
    try:
        loc = geolocator.geocode(name)
    except (GeocoderTimedOut, GeocoderServiceError) as exc:
        raise LocationNotFound(
            f"Could not look up {name!r} right now: the geocoding service did not "
            f"respond. Please try again, or enter a latitude and longitude."
        ) from exc
    if loc is None:
        raise LocationNotFound(
            f"No place called {name!r} was found. Check the spelling, or enter a "
            f"latitude and longitude instead."
        )
    return loc.latitude, loc.longitude

def getCoordinates(opts):
    latitude = 0
    longitude = 0
    altitude = -999
    location = None
    for opt, arg in opts:
        if opt == "-h":
            print("downloadJsonData.py --location 'Braunschweig, Germany'")
            print("downloadJsonData.py --lat 20 --lon 10")
            sys.exit(0)
        elif opt == "--location":
            #print("location", arg)
            location = arg
            latitude, longitude = geocodeLocation(arg)
            print(latitude, longitude)
        elif opt == "--lat":
            latitude = float(arg)
        elif opt == "--lon":
            longitude = float(arg)
    altitude = getElevation(latitude, longitude)
    if altitude is None:
        altitude = UNKNOWN_ELEVATION
    print(altitude)
    return ( latitude, longitude, altitude, location )


import openmeteo_requests

from openmeteo_sdk.Variable import Variable
from openmeteo_sdk.Aggregation import Aggregation

import pandas as pd
import requests_cache
from retry_requests import retry

class TimeoutCachedSession(requests_cache.CachedSession):
    """CachedSession that applies a default timeout.

    requests.Session has no timeout setting, and openmeteo_requests does not pass
    one, so without this a stalled Open-Meteo connection blocks forever.
    """

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", OPEN_METEO_TIMEOUT)
        return super().request(*args, **kwargs)


# Setup the Open-Meteo API client with cache and retry on error
cache_session = TimeoutCachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://ensemble-api.open-meteo.com/v1/ensemble"
def getData(longitude, latitude, altitude, writeToFile = True, meteogram = "10days", metadata = None):
    """Fetch the ensemble and reduce it to the allMeteogramData dict.

    `metadata`, if given, is filled with what the response says about the grid
    cell that was actually used - elevation and timezone - which saves callers a
    separate elevation lookup.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m", "cloud_cover"],
        "models": "ecmwf_ifs025",
        "timezone": "auto",
        "forecast_days": 14
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    if metadata is not None:
        metadata["elevation"] = response.Elevation()
        metadata["latitude"] = response.Latitude()
        metadata["longitude"] = response.Longitude()

    # Process hourly data
    hourly = response.Hourly()
    hourly_variables = list(map(lambda i: hourly.Variables(i), range(0, hourly.VariablesLength())))
    hourly_temperature_2m = filter(lambda x: x.Variable() == Variable.temperature and x.Altitude() == 2, hourly_variables)
    hourly_precipitation = filter(lambda x: x.Variable() == Variable.precipitation, hourly_variables)
    hourly_wind_speed_10m = filter(lambda x: x.Variable() == Variable.wind_speed and x.Altitude() == 10, hourly_variables)
    hourly_cloud_cover = filter(lambda x: x.Variable() == Variable.cloud_cover, hourly_variables)

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    # Process all members
    for variable in hourly_temperature_2m:
        member = variable.EnsembleMember()
        hourly_data[f"temperature_2m_member{member}"] = variable.ValuesAsNumpy()
    for variable in hourly_precipitation:
        member = variable.EnsembleMember()
        hourly_data[f"precipitation_member{member}"] = variable.ValuesAsNumpy()
    for variable in hourly_wind_speed_10m:
        member = variable.EnsembleMember()
        hourly_data[f"wind_speed_10m_member{member}"] = variable.ValuesAsNumpy()
    for variable in hourly_cloud_cover:
        member = variable.EnsembleMember()
        hourly_data[f"cloud_cover_member{member}"] = variable.ValuesAsNumpy()

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    allMeteogramData = {"2t": create_dictionary(
        calculate_percentiles(
            hourly_dataframe,
            column_string="temperature_2m"),
        "2t",
        step_interval=6
    ),
    # Precipitation is an hourly accumulation, so it is summed into 6-hourly
    # buckets first and then passed through with step_interval=1, rather than
    # being subsampled like the instantaneous variables below.
    "tp": create_dictionary(
        calculate_percentiles(
            accumulate_over_steps(hourly_dataframe, "precipitation"),
            column_string="precipitation"),
        "tp",
        step_interval=1
    ),
    "tcc": create_dictionary(
        calculate_percentiles(
            hourly_dataframe,
            column_string="cloud_cover"),
        "tcc",
        step_interval=6
    ),
    "ws": create_dictionary(
        calculate_percentiles(
            hourly_dataframe,
            column_string="wind_speed_10m"),
        "ws",
        step_interval=6
    )
    }
    if writeToFile:
        with open("allmeteogramdata.json", "w") as fp:
            json.dump(allMeteogramData, fp)
    return allMeteogramData

if __name__ == '__main__':
    latitude = 0
    longitude = 0
    altitude = -999
    startTime = time.time()
    if len(sys.argv) > 1:
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hd:", ["days=", "location=", "lat=","lon="])
            latitude, longitude, altitude, _ = getCoordinates(opts)
        except getopt.GetoptError:
            print("downloadJsonData.py --location 'Braunschweig, Germany'")
            print("downloadJsonData.py --lat 20 --lon 10")
            sys.exit(2)
    else:
        location = "Braunschweig Germany"
        latitude, longitude = geocodeLocation(location)
        #print(opts)
        latitude, longitude, altitude, _ = getCoordinates(opts)
    midTime = time.time()
    print(latitude)
    print(longitude)
    allMeteogramData = getData(longitude, latitude, altitude)
    print(allMeteogramData)
    endTime = time.time()
    print("starting up: ", midTime-startTime)
    print("downloading: ", endTime-midTime)
