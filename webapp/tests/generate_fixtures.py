"""Regenerate the offline test fixtures in tests/fixtures/.

Run from webapp/ when the expected Open-Meteo response format changes, or to
refresh stale forecast data:

    python tests/generate_fixtures.py

Each fixture is the verbatim ``allMeteogramData`` dict that downloadJsonData.getData
returns, so the offline tests exercise exactly the structure the live pipeline
produces. Location metadata lives separately in fixtures/locations.json to keep the
fixture files faithful to the real format.

It also records one raw Open-Meteo response, exactly as the new
sources.open_meteo adapter requests it, to fixtures/open_meteo/. The adapter
tests replay those bytes, and so does the parity test that decodes them with
both the adapter and the legacy getData.
"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

from core.model import Location
from meteogram.downloadJsonData import getData
from sources.open_meteo import OpenMeteoEnsemble

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"

# Deliberately diverse regimes so the pictogram thresholds (calm/storm, dry/wet,
# clear/overcast) are all reachable from offline data.
LOCATIONS = {
    "braunschweig": {
        "name": "Braunschweig, Germany",
        "latitude": 52.2646577,
        "longitude": 10.5236066,
        "altitude": 79,
        "timezone": "Europe/Berlin",
        "note": "project default location, temperate maritime",
    },
    "reykjavik": {
        "name": "Reykjavik, Iceland",
        "latitude": 64.1466,
        "longitude": -21.9426,
        "altitude": 61,
        "timezone": "Atlantic/Reykjavik",
        "note": "high latitude, windy and wet - exercises storm/rain pictograms",
    },
    "singapore": {
        "name": "Singapore",
        "latitude": 1.3521,
        "longitude": 103.8198,
        "altitude": 15,
        "timezone": "Asia/Singapore",
        "note": "equatorial, near-zero UTC-offset-independent seasonality, heavy convective rain",
    },
    "zermatt": {
        "name": "Zermatt, Switzerland",
        "latitude": 46.0207,
        "longitude": 7.7491,
        "altitude": 1608,
        "timezone": "Europe/Zurich",
        "note": "alpine, high altitude, large temperature spread",
    },
    "alice_springs": {
        "name": "Alice Springs, Australia",
        "latitude": -23.6980,
        "longitude": 133.8807,
        "altitude": 545,
        "timezone": "Australia/Darwin",
        "note": "southern hemisphere, arid - exercises the no-rain/clear-sky pictograms",
    },
}


# The raw response: short, so the file stays small, but long enough for a
# couple of days of 6-hour steps.
RAW_KEY = "reykjavik"
RAW_DAYS = 3
RAW_FILE = FIXTURE_DIR / "open_meteo" / f"{RAW_KEY}_{RAW_DAYS}d.fb"


def record_raw_response():
    loc = LOCATIONS[RAW_KEY]
    source = OpenMeteoEnsemble(forecast_days=RAW_DAYS)
    params = source.params(
        Location(lat=loc["latitude"], lon=loc["longitude"], name=loc["name"]), source.variables
    )
    response = httpx.get(source.url, params=params, timeout=60)
    response.raise_for_status()
    RAW_FILE.parent.mkdir(exist_ok=True)
    RAW_FILE.write_bytes(response.content)
    print(f"wrote {RAW_FILE} ({len(response.content) / 1024:.0f} KiB)")


def main():
    FIXTURE_DIR.mkdir(exist_ok=True)
    for key, loc in LOCATIONS.items():
        print(f"--- {key}: {loc['name']} ---")
        # NB: getData takes longitude FIRST.
        data = getData(
            loc["longitude"],
            loc["latitude"],
            loc["altitude"],
            writeToFile=False,
        )
        target = FIXTURE_DIR / f"{key}.json"
        with open(target, "w") as fp:
            json.dump(data, fp, indent=1, sort_keys=True)
        print(f"wrote {target} ({os.path.getsize(target) / 1024:.0f} KiB)")

    with open(FIXTURE_DIR / "locations.json", "w") as fp:
        json.dump(LOCATIONS, fp, indent=1, sort_keys=True)
    print(f"wrote {FIXTURE_DIR / 'locations.json'}")
    record_raw_response()


if __name__ == "__main__":
    main()
