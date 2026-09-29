"""Regenerate the offline test fixtures in tests/fixtures/ from the live API.

Run from webapp/ to refresh stale forecast data or after a change to the data
model:

    python tests/generate_fixtures.py                 # every location
    python tests/generate_fixtures.py reykjavik zermatt

Each fixture is a Forecast in the new format - exactly what the pipeline
builds from sources.open_meteo, before any pictograms - so the offline tests
and the offline configuration run on the real thing. sources/fixture.py still
reads the legacy allMeteogramData files recorded before; running this replaces
them. The screenshot baselines of the frontend depend on the fixtures and have
to be re-recorded afterwards (npm run e2e -- --update-snapshots).

Location metadata lives in fixtures/locations.json. It also records one raw
Open-Meteo response, exactly as the adapter requests it, to fixtures/open_meteo/
for the adapter tests to replay.
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

from core.model import Location
from core.pipeline import build_forecast
from sources.open_meteo import OpenMeteoEnsemble
from vsup.config import load as load_vsup

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
# couple of days of 6-hour steps. Reykjavik because the window checks need rain.
RAW_KEY = "reykjavik"
RAW_DAYS = 3
RAW_FILE = FIXTURE_DIR / "open_meteo" / f"{RAW_KEY}_{RAW_DAYS}d.fb"


def location(key):
    loc = LOCATIONS[key]
    return Location(lat=loc["latitude"], lon=loc["longitude"], name=loc["name"])


async def record(key, directory=FIXTURE_DIR):
    source = OpenMeteoEnsemble()
    result = await source.fetch(location(key), source.variables)
    forecast = build_forecast(result, quantile_levels=load_vsup().quantiles)
    target = Path(directory) / f"{key}.json"
    target.write_text(json.dumps(forecast.model_dump(mode="json"), indent=1) + "\n")
    print(f"wrote {target} ({len(forecast.steps)} steps from {forecast.steps[0].isoformat()})")


def record_raw_response(directory=FIXTURE_DIR):
    source = OpenMeteoEnsemble(forecast_days=RAW_DAYS)
    response = httpx.get(source.url, params=source.params(location(RAW_KEY), source.variables), timeout=60)
    response.raise_for_status()
    target = Path(directory) / "open_meteo" / RAW_FILE.name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(response.content)
    print(f"wrote {target} ({len(response.content) / 1024:.0f} KiB)")


def main(keys, directory=FIXTURE_DIR):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    unknown = set(keys) - set(LOCATIONS)
    if unknown:
        sys.exit(f"unknown location(s): {', '.join(sorted(unknown))}; there are {', '.join(LOCATIONS)}")
    for key in keys or LOCATIONS:
        asyncio.run(record(key, directory))
    with open(directory / "locations.json", "w") as fp:
        json.dump(LOCATIONS, fp, indent=1, sort_keys=True)
    print(f"wrote {directory / 'locations.json'}")
    if not keys:
        record_raw_response(directory)


if __name__ == "__main__":
    main(sys.argv[1:])
