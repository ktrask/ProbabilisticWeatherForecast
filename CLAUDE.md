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

`searchForm` is submitted with `method="get"`, so `validate_on_submit()` is **always False** here.
`/search` binds the form to the query string explicitly (`searchForm(request.args)`) and calls
`form.validate()`; that is the only reason the field validators run at all. Validate new parameters
by declaring them on the form, not with hand-rolled `request.args[...]` reads — the latter is what
let an unchecked `plotType` reach the renderer and 500.

Rejections re-render `index.html` with a 400 and the form intact. `quick_form` shows per-field
errors itself, but **not** for a `RadioField`, so anything the user would otherwise not see is
passed to the template as `error` and drawn as an alert.

`app/downloadJsonData.py` and `app/plotMeteogram.py` are **symlinks** to the `webapp/` copies so the
`app` package can import them relatively. The Dockerfile instead copies the real files into
`/app/app/`. Edit the originals in `webapp/`, never the symlinks. (`app/pictogram` is a stale broken
symlink to a non-existent repo-root `pictogram/`; it is unused — the plot code loads
`./pictogram/...` relative to the CWD.)

**The symlinks make each file importable under two module names**, and Python treats
`downloadJsonData` and `app.downloadJsonData` as separate modules with separate class objects. So
`except LocationNotFound` in `views.py` catches only `app.downloadJsonData.LocationNotFound`, and an
`isinstance` check against the top-level class silently fails. The webapp only ever uses the `app.`
path, so this is invisible in production — but tests that touch exception types or class identity
must import from `app.…` to match.

### The `allMeteogramData` format

The contract between layers. Top-level keys are ECMWF-style variable names — `2t` (temperature),
`tp` (precipitation), `tcc` (cloud cover), `ws` (wind speed) — each mapping to
`{<varname>: {min, ten, twenty_five, median, seventy_five, ninety, max, steps}, date: "YYYYMMDD", time: "HHMM"}`.
Note the doubled nesting: `allMeteogramData['tp']['tp']['median']`. Percentile lists are parallel to
`steps` (hours offset from `date`/`time`, currently 6-hourly).

`2t`, `tcc` and `ws` are instantaneous samples at each step — `create_dictionary` subsamples the
hourly frame with `df.iloc[::6]`. `tp` is different: it is the rainfall **accumulated across** the
step, summed per member by `accumulate_over_steps()` before percentiles are taken, then passed to
`create_dictionary` with `step_interval=1`. The summing must happen per member and before the
percentiles — the 90th percentile of 6-hour totals is not the sum of hourly 90th percentiles. Any
new accumulated variable needs the same treatment; subsampling one silently discards 5/6 of it.

### Pictogram selection

`getVSUP*Coordinate()` / `getHres*Coordinate()` map a single timestep's percentiles to an index into
a fixed filename list under `pictogram/{rain,wind,cloud}/` (7 files, ensemble) or
`pictogram/*/enhanced_hres/` (16 files, 4 certainty levels × 4 intensity levels). Adding or reordering
a pictogram means updating both the coordinate function's thresholds and the filename list in the
matching `plot*VSUP` function — they are positionally coupled.

Cloud cover thresholds are in percent (10 / 30 / 50 / 70 / 90) and precipitation in mm per 6-hour
step (0.1 / 1 / 1.5 / 2), both matching what the pipeline delivers. Wind (m/s: 3 / 10 / 17.2) is
still written in the old grib pipeline's units and does **not** match the API — see "Wind units"
under Known gaps.

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

- **`enhanced-hres` cannot be served from Open-Meteo data.** It needs a `hres` key (deterministic
  high-res run) that `downloadJsonData.create_dictionary` never produces. `plotMeteogram()` checks
  for it up front and raises `HresDataUnavailable`, which `/search` turns into a 503 with an
  explanation — the request is valid, the data source just cannot fulfil it. It worked with the older
  ECMWF grib pipeline that commit `cc18d28` removed. The radio button is still offered; restore an
  HRES source or remove the choice.
- **The 15-day daily branch is dead.** `getData(..., meteogram="15days")` ignores the argument and
  always returns 6-hourly `tp`/`2t` keys, so the `tp24`/`mn2t24`/`mx2t24` paths in
  `plotMeteogram()` and `getTimeFrame()` are unreachable. `controller.py` still branches on
  `days > 10`.
- The repo-root `requirements.txt` belongs to the removed ECMWF grib/metview data API and is not what
  the webapp installs; `webapp/requirements.txt` is the live one.
- A plotly rewrite of the renderer was started twice and dropped both times (commit `3e00557`, and an
  untracked `app/plotMeteogram_plotly.py` deleted on 2026-09-18). The repo-root `plotly.html` is a
  leftover sample output. Rendering is matplotlib-only; `plotly` is not in `requirements.txt`.
- **Wind units do not match the thresholds.** Open-Meteo delivers wind in km/h, but
  `getVSUPWindCoordinate` / `getHresWindCoordinate` still use the grib pipeline's m/s,
  so every threshold fires 3.6× too early and a 4.8 m/s breeze is drawn as a storm.
  Covered by `tests/test_pictograms.py::TestUnitMismatch` as an xfail. Cloud cover and
  precipitation had the same defect and were fixed by restating their thresholds in the
  delivered unit; fix wind the same way rather than converting the data.
- `controller.py` ignores the computed `fromIndex` (hardcodes `0`), and `plotMeteogram()` overrides it
  to `1`, so meteograms always start at the forecast's second step rather than "now".

## Conventions

- Every outbound call sets an explicit timeout: `requests` has no default, and `requests.Session`
  offers no way to set one, so `TimeoutCachedSession` injects `OPEN_METEO_TIMEOUT` into
  `session.request()` — `openmeteo_requests` never passes one itself. Keep that wrapper in place;
  without it a stalled forecast call blocks a worker indefinitely.
- Elevation is decoration on the plot title, so a failed lookup degrades to `UNKNOWN_ELEVATION`
  (-999) rather than failing the forecast. The web path takes elevation from `getData`'s `metadata`
  (what Open-Meteo reports for the grid cell it used); `getElevation`'s call to open-elevation.com
  is only used by the CLI.
- A place name that cannot be resolved raises `LocationNotFound`, which `/search` turns into a 400
  naming what the user typed. Do not let `geocode()` return `None` into a forecast request.

- `plotMeteogram()` and `plotTemperature()` reject an unknown `plotType` with `ValueError` rather
  than falling through their `if/elif` chains. Keep the explicit `else: raise` — without it the
  failure surfaces as `UnboundLocalError: localMinima` from deep inside the temperature panel.

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
