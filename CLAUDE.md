# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Probabilistic weather meteograms, based on the 2018 ECMWF European Summer of Weather Code project.
The core idea is VSUP (Value-Suppressing Uncertainty Palette) encoding: ensemble spread is rendered
as pictograms whose visual specificity shrinks as forecast uncertainty grows, instead of plotting a
single deterministic line.

## Commands

Everything runs from `webapp/`. Relative paths (`./pictogram/...`, `output/`, `.cache.sqlite`) are
resolved against the CWD, so running from elsewhere breaks image loading.

```bash
cd webapp
pip install -r requirements.txt

python run.py                      # Flask dev server, debug on, 0.0.0.0:5003

# Render a meteogram PNG to ./output/forecast.png without the webapp:
python plotMeteogram.py --location 'Braunschweig, Germany' --days 10 --ensemble
python plotMeteogram.py --lat 52.26 --lon 10.52 --days 10 --ensemble
python plotMeteogram.py            # no args: replays webapp/allmeteogramdata.json (offline)

# Fetch + cache ensemble data to allmeteogramdata.json:
python downloadJsonData.py --location 'Braunschweig, Germany'

docker build -t meteogram . && docker run -p 5003:5003 meteogram
```

### Tests

```bash
pip install -r requirements-dev.txt

pytest                             # everything, incl. the live API format check
pytest -m "not live"               # offline only - no network at all
pytest -m live                     # only the Open-Meteo format check
pytest tests/test_pictograms.py -k thresholds   # a single group
pytest tests/test_plotting.py::TestGetTimeFrame::test_full_range_from_reference_time

python tests/generate_fixtures.py  # refresh tests/fixtures/ from the live API
```

`tests/conftest.py` chdirs to `webapp/` at import, so pytest works from any
directory. Live tests skip themselves when the network is unreachable. There are no
linters or CI.

## Architecture

Three layers, deliberately decoupled by a plain-dict data format:

1. **`downloadJsonData.py`** — fetches the ECMWF IFS 0.25° 51-member ensemble from the Open-Meteo
   ensemble API, reduces each member set to percentiles, and emits the `allMeteogramData` dict.
2. **`plotMeteogram.py`** — pure rendering. Takes `allMeteogramData` + index range + timezone +
   plot type, returns a matplotlib `Figure`. Never touches the network.
3. **`app/`** — thin Flask layer. `views.py` (form + routes) → `controller.py` (geocode, fetch,
   plot, save to `/tmp`, read back as base64, delete) → template renders the PNG inline. No DB;
   `models.py` is empty and the `flask_sqlalchemy` wiring in `app/__init__.py` is commented out.

`app/downloadJsonData.py` and `app/plotMeteogram.py` are **symlinks** to the `webapp/` copies so the
`app` package can import them relatively. The Dockerfile instead copies the real files into
`/app/app/`. Edit the originals in `webapp/`, never the symlinks. (`app/pictogram` is a stale broken
symlink to a non-existent repo-root `pictogram/`; it is unused — the plot code loads
`./pictogram/...` relative to the CWD.)

### The `allMeteogramData` format

The contract between layers. Top-level keys are ECMWF-style variable names — `2t` (temperature),
`tp` (precipitation), `tcc` (cloud cover), `ws` (wind speed) — each mapping to
`{<varname>: {min, ten, twenty_five, median, seventy_five, ninety, max, steps}, date: "YYYYMMDD", time: "HHMM"}`.
Note the doubled nesting: `allMeteogramData['tp']['tp']['median']`. Percentile lists are parallel to
`steps` (hours offset from `date`/`time`, currently 6-hourly).

### Pictogram selection

`getVSUP*Coordinate()` / `getHres*Coordinate()` map a single timestep's percentiles to an index into
a fixed filename list under `pictogram/{rain,wind,cloud}/` (7 files, ensemble) or
`pictogram/*/enhanced_hres/` (16 files, 4 certainty levels × 4 intensity levels). Adding or reordering
a pictogram means updating both the coordinate function's thresholds and the filename list in the
matching `plot*VSUP` function — they are positionally coupled.

Cloud cover thresholds are in percent (10 / 30 / 50 / 70 / 90), matching what Open-Meteo delivers.
Wind (m/s: 3 / 10 / 17.2) and precipitation (metres: 1e-4 … 2e-3) are still written in the old grib
pipeline's units and do **not** match the API — see "Wind and precipitation units" under Known gaps.

## Tests

`tests/schema.py` holds `assert_meteogram_schema()`, the single definition of the
`allMeteogramData` contract. Both the offline fixture tests and the live API test call
it, so upstream format drift surfaces as the same failure either way — extend that
function rather than adding ad-hoc assertions.

- `tests/fixtures/*.json` are verbatim `getData()` outputs for five deliberately
  different climates (temperate, subarctic, equatorial, alpine, arid) so the pictogram
  branches are reachable offline. `locations.json` holds the lat/lon/timezone metadata,
  kept out of the fixtures so they stay faithful to the real format.
- Tests derive dates from each fixture's own reference time, never `utcnow()`, so they
  stay deterministic as fixtures age. Fixtures do go stale as forecasts — regenerate
  them if you need current weather, not for correctness.
- Known bugs are recorded as `strict=True` xfails, so they flip to a loud XPASS the
  moment someone fixes them.

## Known gaps

- **`enhanced-hres` plot type is broken against Open-Meteo data.** It reads a `hres` key
  (deterministic high-res run) that `downloadJsonData.create_dictionary` never produces, so it raises
  `KeyError`. It worked with the older ECMWF grib pipeline that commit `cc18d28` removed. Use
  `ensemble` unless you are restoring an HRES source.
- **The 15-day daily branch is dead.** `getData(..., meteogram="15days")` ignores the argument and
  always returns 6-hourly `tp`/`2t` keys, so the `tp24`/`mn2t24`/`mx2t24` paths in
  `plotMeteogram()` and `getTimeFrame()` are unreachable. `controller.py` still branches on
  `days > 10`.
- `app/plotMeteogram_plotly.py` is an in-progress, untracked port following
  `.posit/assistant/plans/2026-07-22-2025-plan.md`. It is not wired into `controller.py`, and neither
  `plotly` nor `kaleido` is in `requirements.txt`.
- The repo-root `requirements.txt` belongs to the removed ECMWF grib/metview data API and is not what
  the webapp installs; `webapp/requirements.txt` is the live one.
- **Wind and precipitation units do not match the thresholds.** Open-Meteo delivers
  precipitation in mm and wind in km/h, but `getVSUPrainCoordinate` /
  `getVSUPWindCoordinate` (and their `getHres*` twins) still use the grib pipeline's
  metres and m/s. So any measurable rain clears the `2e-3` "strong rain" threshold —
  making the light/medium rain pictograms (indices 4 and 5) unreachable — and wind is
  over-reported by a factor of 3.6, putting a 4.8 m/s breeze over the "storm" line.
  Covered by `tests/test_pictograms.py::TestUnitMismatch` as an xfail. Cloud cover had
  the same defect and was fixed by restating its thresholds in percent; fix these the
  same way rather than converting the data.
- `app/plotMeteogram_plotly.py` still carries the pre-fix 0–1 cloud thresholds. Port
  the percent change across before wiring it in.
- `controller.py` ignores the computed `fromIndex` (hardcodes `0`), and `plotMeteogram()` overrides it
  to `1`, so meteograms always start at the forecast's second step rather than "now".

## Conventions

- `matplotlib.use('Agg')` is set at import in `plotMeteogram.py` — required, Tk is not thread-safe
  under Flask. Don't switch backends.
- Text rendering prefers `~/.fonts/BebasNeue Regular.otf` and silently falls back to DejaVu Sans.
  The shared `prop` FontProperties object is mutated in place (`prop.set_size`) and restored — keep
  that pattern if you touch title sizing.
- Figures must be closed (`pltclose`) after saving in request handlers; the webapp leaks otherwise.
- Open-Meteo responses are cached in `webapp/.cache.sqlite` for 1 hour via `requests_cache`. Delete it
  to force a refetch while debugging.
- Geocoding uses Nominatim with user agent `ESOWC-Meteogram-2018`; elevation comes from
  open-elevation.com and falls back to `-999`.
