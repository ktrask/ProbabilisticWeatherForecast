from datetime import date, timedelta, datetime
import time, json, sys
from pathlib import Path
from geopy.geocoders import Nominatim
import asyncio, async_timeout, aiohttp
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

def getElevation(latitude: int,longitute: int):
    baseUrl = "https://api.open-elevation.com"
    response = requests.get(f"{baseUrl}/api/v1/lookup?locations={latitude},{longitute}")
    result = json.loads(response.text)
    return(result["results"][0]["elevation"])

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
            geolocator = Nominatim(user_agent="ESOWC-Meteogram-2018")
            loc = geolocator.geocode(arg)
            latitude = loc.latitude
            longitude = loc.longitude
            print(latitude, longitude)
        elif opt == "--lat":
            latitude = float(arg)
        elif opt == "--lon":
            longitude = float(arg)
    altitude = getElevation(latitude, longitude)
    if altitude is None:
        altitude = -999
    print(altitude)
    return ( latitude, longitude, altitude, location )


import openmeteo_requests

from openmeteo_sdk.Variable import Variable
from openmeteo_sdk.Aggregation import Aggregation

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://ensemble-api.open-meteo.com/v1/ensemble"
def getData(longitude, latitude, altitude, writeToFile = True, meteogram = "10days"):
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
    "tp": create_dictionary(
        calculate_percentiles(
            hourly_dataframe,
            column_string="precipitation"),
        "tp",
        step_interval=6
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
        geolocator = Nominatim(user_agent="ESOWC-Meteogram-2018")
        loc = geolocator.geocode(location)
        latitude = loc.latitude
        longitude = loc.longitude
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
