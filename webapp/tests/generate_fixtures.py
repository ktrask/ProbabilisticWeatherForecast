"""Regenerate the offline test fixtures in tests/fixtures/ from the live API.

Run from webapp/ to refresh stale forecast data or after a change to the data
model:

    python tests/generate_fixtures.py                 # every location
    python tests/generate_fixtures.py reykjavik zermatt
    python tests/generate_fixtures.py --model icon_global_eps   # another product's
    python tests/generate_fixtures.py --model icon_d2_eps --step 1   # in hourly steps

Each fixture is a Forecast in the new format - exactly what the pipeline
builds from sources.open_meteo, with the quantile levels vsup.yaml computes and
before any pictograms - so the offline tests and the offline configuration run
on the real thing. The legacy-format sample in fixtures/legacy/ is not touched.
The screenshot baselines of the frontend depend on the fixtures and have to be
re-recorded afterwards (npm run e2e -- --update-snapshots).

Location metadata lives in fixtures/locations.json. Without arguments it also
records one raw Open-Meteo response, exactly as the adapter requests it, to
fixtures/open_meteo/ for the adapter tests to replay; with location keys only
those fixtures are re-recorded.

Every product of config/sources.yaml has its recordings in its own directory
(MODELS below), which config/sources.fixtures.yaml serves under the same
product name. ECMWF's, the default, are the ones directly in fixtures/.
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

from core.model import Location
from core.pipeline import build_forecast
from sources.base import NotCovered
from sources.open_meteo import OpenMeteoEnsemble
from vsup.config import load as load_vsup

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"

# Open-Meteo model -> (where its recordings go, how many days to ask for).
DEFAULT_MODEL = "ecmwf_ifs025"
MODELS = {
    DEFAULT_MODEL: (FIXTURE_DIR, 15),
    "icon_global_eps": (FIXTURE_DIR / "icon_eps", 8),
    "ecmwf_aifs025": (FIXTURE_DIR / "aifs", 16),
    "gfs_seamless": (FIXTURE_DIR / "gfs", 16),
    "ncep_aigefs025": (FIXTURE_DIR / "aigefs", 16),
    "gem_global": (FIXTURE_DIR / "gem", 16),
    "icon_eu_eps": (FIXTURE_DIR / "icon_eu_eps", 6),
    "icon_d2_eps": (FIXTURE_DIR / "icon_d2_eps", 3),
    "meteoswiss_icon_ch2": (FIXTURE_DIR / "meteoswiss_ch2", 6),
}

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


async def record(key, directory=FIXTURE_DIR, model=DEFAULT_MODEL, step_hours=6):
    """Record one location; False if the model does not cover it. Steps other
    than 6 hours go to <key>.<n>h.json beside the 6-hour <key>.json."""
    source = OpenMeteoEnsemble(model, forecast_days=MODELS[model][1])
    try:
        result = await source.fetch(location(key), source.variables)
    except NotCovered:
        print(f"skipped {key}: {model} does not cover it")
        return False
    forecast = build_forecast(result, step_hours=step_hours, quantile_levels=load_vsup().quantiles)
    target = Path(directory) / (f"{key}.json" if step_hours == 6 else f"{key}.{step_hours}h.json")
    target.write_text(json.dumps(forecast.model_dump(mode="json"), indent=1) + "\n")
    print(f"wrote {target} ({len(forecast.steps)} steps from {forecast.steps[0].isoformat()})")
    return True


def record_raw_response(directory=FIXTURE_DIR):
    source = OpenMeteoEnsemble(forecast_days=RAW_DAYS)
    response = httpx.get(source.url, params=source.params(location(RAW_KEY), source.variables), timeout=60)
    response.raise_for_status()
    target = Path(directory) / "open_meteo" / RAW_FILE.name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(response.content)
    print(f"wrote {target} ({len(response.content) / 1024:.0f} KiB)")


def main(keys, directory=None, model=DEFAULT_MODEL, step_hours=6):
    directory = Path(directory) if directory else MODELS[model][0]
    directory.mkdir(parents=True, exist_ok=True)
    unknown = set(keys) - set(LOCATIONS)
    if unknown:
        sys.exit(f"unknown location(s): {', '.join(sorted(unknown))}; there are {', '.join(LOCATIONS)}")
    recorded = [key for key in keys or LOCATIONS if asyncio.run(record(key, directory, model, step_hours))]
    # A regional model's directory lists only the places it covers. With keys,
    # the ones recorded before stay listed.
    index = directory / "locations.json"
    listed = set(json.loads(index.read_text())) if keys and index.exists() else set()
    with open(index, "w") as fp:
        json.dump({k: LOCATIONS[k] for k in LOCATIONS if k in listed | set(recorded)}, fp, indent=1, sort_keys=True)
    print(f"wrote {directory / 'locations.json'}")
    if not keys and model == DEFAULT_MODEL and step_hours == 6:
        record_raw_response(directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("keys", nargs="*", help=f"locations; default: all of {', '.join(LOCATIONS)}")
    parser.add_argument("--model", choices=sorted(MODELS), default=DEFAULT_MODEL)
    parser.add_argument("--step", type=int, default=6, help="step width in hours (default 6)")
    args = parser.parse_args()
    main(args.keys, model=args.model, step_hours=args.step)
