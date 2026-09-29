# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Probabilistic weather meteograms, based on the 2018 ECMWF European Summer of Weather Code project.
The core idea is VSUP (Value-Suppressing Uncertainty Palette) encoding: ensemble spread is rendered
as pictograms whose visual specificity shrinks as forecast uncertainty grows, instead of plotting a
single deterministic line.

## Commands

Run from `webapp/`. The only thing still resolved against the CWD is the requests_cache store
(`.cache.sqlite`) and the CLI's `output/` directory; pictograms and code are found relative to the
package, so imports work from anywhere.

```bash
cd webapp
pip install -r requirements.txt

python run.py                      # Flask dev server, 127.0.0.1:5003, debug OFF
FLASK_DEBUG=1 python run.py        # debug on; refused unless bound to loopback

# Render a meteogram PNG to ./output/forecast.png without the webapp:
python -m meteogram.plotMeteogram --location 'Braunschweig, Germany' --days 10 --ensemble
python -m meteogram.plotMeteogram --lat 52.26 --lon 10.52 --days 10 --ensemble
python -m meteogram.plotMeteogram  # no args: replays webapp/allmeteogramdata.json (offline)

# Fetch + cache ensemble data to allmeteogramdata.json:
python -m meteogram.downloadJsonData --location 'Braunschweig, Germany'

# The new backend (see "The rework" below):
python -m api serve                           # FastAPI dev server, 127.0.0.1:8000, docs at /api/docs
SOURCES_CONFIG=config/sources.fixtures.yaml python -m api serve   # offline: recorded forecasts
python -m vsup check                          # validate config/vsup.yaml + rule coverage on the fixtures
python -m sources check                       # validate config/sources.yaml against the schemes
python -m vsup schema -o config/vsup.schema.json        # after changing vsup/config.py's models
python -m sources schema -o config/sources.schema.json  # after changing sources/config.py's models
python -m api openapi -o api/openapi.json               # after changing a route or response model

# The new frontend (webapp/frontend/, needs Node 22):
cd frontend && npm ci
npm run dev                        # Vite on :5173, proxies /api and /pictograms to 127.0.0.1:8000
npm run build                      # tsc + vite build -> dist/
npm test                           # Vitest unit/component tests (jsdom)
npm run e2e                        # Playwright; starts the offline API and a preview build itself
npm run e2e -- --update-snapshots  # after an intended visual change - review the PNG diff
npm run gen:api                    # src/api/schema.d.ts from api/openapi.json; check:api verifies it
npx playwright install chromium    # once, for the pinned browser the screenshots are taken with

docker build -t meteogram . && docker run -p 5003:5003 -e SECRET_KEY="$(openssl rand -hex 32)" meteogram
```

The container serves with gunicorn (`startup.sh`), not `run.py`. Environment: `SECRET_KEY`,
`HOST`/`PORT`, `WEB_CONCURRENCY`, `GUNICORN_TIMEOUT`, and `FLASK_DEBUG`/`ALLOW_PUBLIC_DEBUG` for
`run.py` only. The new API reads `VSUP_CONFIG`, `SOURCES_CONFIG`, `FORECAST_CACHE_TTL_S`,
`FORECAST_CACHE_SIZE`, `GEOCODE_CACHE_TTL_S`, `GEOCODE_CACHE_SIZE`, `GEOCODER_TIMEOUT_S` (see
`api/settings.py`); nothing serves it in the container yet.

All CLIs must be run with `-m`: they are package modules and import their siblings by package path.

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

Two packages under `webapp/`: `meteogram/` is the domain (fetch, render, assets) and knows nothing
about the web; `app/` is the Flask layer. Three layers, decoupled by a plain-dict data format:

1. **`meteogram/downloadJsonData.py`** — fetches the ECMWF IFS 0.25° 51-member ensemble from the
   Open-Meteo ensemble API, reduces each member set to percentiles, and emits the
   `allMeteogramData` dict.
2. **`meteogram/plotMeteogram.py`** — pure rendering. Takes `allMeteogramData` + index range +
   timezone + plot type, returns a matplotlib `Figure`. Never touches the network. Loads its
   glyphs from `meteogram/pictogram/`, resolved via `PICTOGRAM_DIR` (`__file__`-relative, not CWD).
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

`app/` previously reached the domain modules through symlinks (`app/downloadJsonData.py` →
`../downloadJsonData.py`), which made each file importable under **two** module names —
`downloadJsonData` and `app.downloadJsonData` — with separate class objects, so `except
LocationNotFound` caught only one of them. The `meteogram/` package replaces that: one file, one
module path, imported as `meteogram.…` everywhere including the Dockerfile. Do not reintroduce a
symlink or a second copy to make an import work.

### The `allMeteogramData` format

The contract between layers. Top-level keys are ECMWF-style variable names — `2t` (temperature),
`tp` (precipitation), `tcc` (cloud cover), `ws` (wind speed at 10 m, in m/s) — each mapping to
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

Cloud cover thresholds are in percent (10 / 30 / 50 / 70 / 90), precipitation in mm per 6-hour
step (0.1 / 1 / 1.5 / 2) and wind in m/s (3 / 10 / 17.2, the last being the bottom of Beaufort 8),
all matching what the pipeline delivers. All three once disagreed with the data — fraction vs
percent, metres vs mm, km/h vs m/s — and none of those mismatches failed; they just picked the
wrong glyphs. Cloud and rain were fixed by restating the thresholds in the delivered unit. Wind
went the other way: the thresholds stay in m/s and `FORECAST_PARAMS` asks Open-Meteo for
`wind_speed_unit=ms` (its default is km/h), because m/s is the unit Beaufort is defined in and the
canonical unit for the planned rework. Do not restate the wind thresholds in km/h.

### The rework: `core/`, `sources/`, `vsup/`, `api/`

A new backend is being built next to the legacy one, which keeps serving until the Flask app is
retired (the plan, in German, is `docs/neuentwicklung-plan.md`). The goal: the backend returns only
JSON data, the browser draws the meteogram, and VSUP rules are configuration rather than code.
Nothing in `app/` uses the new packages. The container still serves only the Flask app.

- **`core/`** — no I/O. `variables.py` lists each variable with its one canonical unit (`degC`,
  `mm`, `percent`, `m/s`) and whether it is an `instant` or a `sum`; `units.py` converts to those
  units or raises. `model.py` is the `Forecast` (Pydantic), successor of `allMeteogramData`: steps
  in UTC, units and aggregation explicit, quantiles named `p0`…`p100`, no doubled nesting. Its
  validators are the contract; `tests/schema.py` still covers the legacy format. `reduce.py` +
  `pipeline.py` turn a `SourceResult` into a `Forecast`.
- **`sources/`** — adapters returning a `SourceResult` in canonical units and UTC.
  `open_meteo.OpenMeteoEnsemble` (async httpx, flatbuffers so values are the same float32 that
  `getData` sees, an explicit timeout on every request, unit read from the response and converted
  or refused). `fixture.FixtureSource` serves `tests/fixtures/` as a quantile source.
  `geocode.OpenMeteoGeocoder` finds places. `config.py` + **`config/sources.yaml`** define sources
  and the *products* the UI offers (a source plus the schemes drawn from it; the first is the
  default); `config/sources.fixtures.yaml` is its offline twin with the same product names.
- **`vsup/`** + **`config/vsup.yaml`** — the schemes. `rules` mode is an ordered list of `when:`
  expressions (own parser in `expr.py`, never `eval`); `tree` mode is classes merging into groups
  as certainty drops. `config.load()` collects *every* problem with its line number before raising
  `ConfigError`. `config/vsup.schema.json` is generated from the models; a test fails if it is stale.
  `pictogram_root` points at `meteogram/pictogram/` for now — no second copy of the assets.
- **`api/`** — FastAPI, GET only: `/api/forecast`, `/api/geocode`, `/api/products`, `/api/schemes`,
  `/api/health`, and `/pictograms/<version>/…`. `create_app()` is a factory (`uvicorn
  api.app:create_app --factory`) so importing it has no side effects; it loads and cross-checks both
  configs, so a broken one stops the start. `api/openapi.json` is the checked-in contract.
- **`frontend/`** — React 19 + TypeScript + Vite, TanStack Query, the chart as SVG with
  d3-scale/d3-shape (no chart library). `src/api/schema.d.ts` is generated from
  `api/openapi.json`, so an API change the frontend does not follow is a type error. `src/state/`
  keeps place/product/variant/days in the URL only (links shareable, back/forward work);
  `src/meteogram/layout.ts` + `time.ts` are the pure logic (window, local days, extremes).

Rules worth knowing:

- The new packages do not import `meteogram` or `app`. Only tests (golden, parity) touch both.
  Layering: `core` <- `sources`, `vsup` <- `api`. `sources.config.load()` checks products against a
  loaded VSUP config handed to it; only the `python -m sources` CLI imports `vsup` itself.
- **Start-up checks replace request-time failures.** `sources.config.load()` refuses a product whose
  scheme is missing, draws a variable the source does not deliver, is written for a window other
  than the 6-hour step, reads `deterministic`, or needs quantiles a recorded source lacks; two
  schemes for one variable are refused too. This is the successor of `HresDataUnavailable`.
- **Errors:** 422 invalid parameter (an unknown `product` too, in FastAPI's error shape), 404
  `NoData` (e.g. no fixture near the place), 409 variant not offered (`hres` everywhere for now),
  502 `SourceError`/`DataGap`, 504 `SourceTimeout`. All bodies are `{"detail": ...}`.
- **Cache:** `api/cache.TTLCache`, in process, per worker. Forecasts 1 h keyed by source, variables
  and coordinates rounded to 2 decimals (the grid is 25 km); geocoding 1 day keyed by the
  normalised query. Concurrent identical requests share one upstream fetch; a caller that hangs up
  does not cancel it; failures are not cached. Pictograms are served under the config's `version`
  hash with `immutable` caching — a changed file or config gets a new URL.
- **Geocoding is Open-Meteo, not Nominatim.** Nominatim's usage policy forbids search-as-you-type
  and anything above 1 request/s; the planned location box searches while typing. The legacy app
  still uses Nominatim through geopy.
- **The chart works in the forecast location's time zone, never the browser's** (`meteogram/time.ts`
  via `Intl`, DST included). Playwright runs with the browser in America/New_York to keep it so.
- **Long meteograms split into sections one below the other** (`layout.sections`): when a step would
  get under 28 px (`MIN_CELL`), but never into sections shorter than 5 days (`MIN_SECTION_DAYS`,
  the user's call - splitting sooner read as too fragmented; on a phone the pictograms shrink
  instead up to 5 days). Cuts are at local midnight, as even as the days allow; neighbours share
  the cut step so the line runs on and the total starting there is drawn once, in the later one.
  All sections share one px-per-step and one temperature scale.
- **Instants sit at their step, totals in the middle of their window**: cloud/wind/temperature at
  `t`, precipitation at `t + 3h`, between two instants. A total is drawn only if its whole window
  fits on the chart, so there is one precipitation pictogram fewer than steps.
- **The view starts at the step nearest to now** (the legacy `fromIndex` bug is gone). A forecast
  that ended before now is shown from its start with a "stale" note - which is what the offline
  fixtures look like once they age; the e2e tests freeze the clock inside the recording.
- **Coordinates in the URL are never rounded on the way back out** (`formatState`): rounding moved
  the place on the first adjustment and refetched the forecast. Whoever picks a place rounds it.
- Screenshot baselines (`frontend/e2e/__screenshots__/`, platform-suffixed) are taken with
  Playwright's pinned Chromium against `sources.fixtures.yaml`; they only mean something on that
  browser and those fixtures - regenerating the fixtures means regenerating the baselines.
- TypeScript is pinned to 5.9 because openapi-typescript 7 requires `typescript ^5`.
- Starlette 1.7 deprecates `TestClient` on httpx. The API tests call the app through
  `httpx.ASGITransport` instead (`tests/test_api.get`); keep it that way.
- **The `*-legacy` schemes are frozen.** `tests/test_vsup_golden.py` holds them to exactly the
  pictograms `getVSUP*Coordinate()` pick, on every fixture step and on an exhaustive grid around
  every threshold (read from the legacy functions' source). Change design in the `*-vsup` tree
  schemes, which are allowed to differ.
- **Sum windows are an hour later than legacy.** Open-Meteo's hourly precipitation is the
  *preceding* hour's total, so the step at `t` covers `[t, t+6h)` = rows `t+1 … t+6`.
  `accumulate_over_steps()` sums rows `t … t+5`. `test_sources.py` pins both against raw members.
- The adapter asks for 15 days; the run ends around hour 350 and Open-Meteo pads the rest with NaN.
  `reduce.complete_rows` drops trailing NaN rows and refuses holes, and all variables share the
  steps a 6-hour *sum* can fill — 58 steps for `ecmwf_ifs025`, where the legacy path had 56.
- Every conversion in `core/units.py` must stay strictly increasing: `vsup` converts a scheme's
  thresholds once at load time, which is only sound if `a < b` survives the conversion.
- `webapp/config/` (data files) sits next to Flask's `webapp/config.py`. `import config` still
  finds the module because the directory has no `__init__.py` — do not add one.

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
- `tests/fixtures/open_meteo/reykjavik_3d.fb` is one raw flatbuffers response, recorded by
  `generate_fixtures.py` with the new adapter's parameters. `test_sources.py` replays it through
  `httpx.MockTransport` and through the legacy `getData`, which is the parity check between the two
  reductions. Reykjavik because it rains there — the precipitation checks need rain.
- New backend: `test_core.py` (units, `Forecast` contract, reduction), `test_sources.py`,
  `test_vsup_expr.py`, `test_vsup_config.py` (one case per validator error, with line numbers),
  `test_vsup_golden.py` (legacy parity), `test_catalog.py` (sources.yaml start-up checks),
  `test_api.py` (routes, status codes, cache, read-only routes, OpenAPI contract). The API tests
  run on `config/sources.fixtures.yaml` and fake failing sources. The live suite also runs the new
  adapter and the API end to end.
- Frontend: Vitest next to the code (`*.test.ts[x]`: layout and time zones incl. the 25-hour DST
  day, URL state, API errors, i18n, legend, search debounce/keyboard/coordinates, meteogram
  rendering and keyboard crosshair) and Playwright in `frontend/e2e/` (screenshots of all five
  fixtures plus a phone, hover, days without refetch, search + back/forward, 404, legend).

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
- `controller.py` ignores the computed `fromIndex` (hardcodes `0`), and `plotMeteogram()` overrides it
  to `1`, so meteograms always start at the forecast's second step rather than "now".
- The legacy precipitation windows are an hour early (see "The rework"). Not fixed in
  `downloadJsonData`, which is going away; the new pipeline gets it right.
- The rework has no HRES yet: `deterministic` parses in `when:` expressions and the model has a slot
  for it, but no adapter delivers one, the `anchored` mode for HRES schemes does not exist, and a
  product cannot name a deterministic source. `variant=hres` answers 409.
- The new API has no rate limiting, and its cache is per worker process.
- Frontend MVP gaps: no 12-hour aggregation (the plan's `step_hours` API parameter - sections
  took its place for now), no SVG/PNG export, light theme only, and the variant/product pickers
  only appear once there is more than one to choose.

## Serving

- **`run.py` is development only** — Flask's built-in server. The container runs
  `gunicorn app:app` from `startup.sh`, which `exec`s so gunicorn is PID 1 and `docker stop`
  actually stops it (verified: exits on SIGTERM in ~1s). It replaced a `while :; do python3 run.py;
  sleep 1; done` loop that restarted after every crash and swallowed the error.
- **Debug is opt-in and refuses a public bind.** `run.py` defaults to `127.0.0.1` with debug off;
  `FLASK_DEBUG=1` with a non-loopback `HOST` raises `SystemExit` unless `ALLOW_PUBLIC_DEBUG=1`.
  That combination exposes the Werkzeug debugger, which is remote code execution for anyone who can
  reach the port and provoke a traceback. A public bind *without* debug is fine and is what the
  container does.
- **`SECRET_KEY` comes from the environment.** If unset, `config.py` generates a random per-process
  key and warns — safe by default, but sessions will not survive a restart or be shared between
  gunicorn workers, so set it before serving traffic. Never commit one; the previous hardcoded key
  is still in the git history.
- **CSRF is off deliberately**, not by oversight: every route is a read-only GET, there is no
  session, login or state change, and enabling it would break `/search`, which binds the form to
  `request.args` where no `csrf_token` exists. `test_deployment.py` fails if any route starts
  accepting POST/PUT/PATCH/DELETE while CSRF is disabled — that is the signal to revisit it.

## Conventions

- Every outbound call sets an explicit timeout: `requests` has no default, and `requests.Session`
  offers no way to set one, so `TimeoutCachedSession` injects `OPEN_METEO_TIMEOUT` into
  `session.request()` — `openmeteo_requests` never passes one itself. Keep that wrapper in place;
  without it a stalled forecast call blocks a worker indefinitely. The new `sources/open_meteo.py`
  passes its `httpx.Timeout` on each request, so a shared client without one cannot undo it.
- `getData()` checks the unit Open-Meteo reports for every member of every variable against
  `EXPECTED_UNITS` and raises `UnexpectedUnit` on a mismatch — asking for a unit is not enough.
  If the API ignored or renamed `wind_speed_unit` (it was `windspeed_unit` once), km/h would reach
  the pictograms silently. A new variable needs an entry there and in the live test's
  `EXPECTED_UNITS`, which checks the JSON API's spelling of the same units. Nothing in the web
  layer catches `UnexpectedUnit`, so it surfaces as a 500 — loud on purpose.
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
- Assets are found through `PICTOGRAM_DIR`, never a `./pictogram/...` literal. The `output/`
  directory is created by the CLI's `__main__` block, not at import — keep import side effects out
  of the package so it stays importable on a read-only filesystem.
- Text rendering prefers `~/.fonts/BebasNeue Regular.otf` and silently falls back to DejaVu Sans.
  The shared `prop` FontProperties object is mutated in place (`prop.set_size`) and restored — keep
  that pattern if you touch title sizing.
- Figures must be closed (`pltclose`) after saving in request handlers; the webapp leaks otherwise.
- Pictograms go through `readPictogram()`, an `lru_cache` over `plt.imread`. Each panel calls
  `imscatter()` once per timestep, so a 14-day meteogram would otherwise decode the same ~15 PNGs
  165 times. The cached arrays are marked read-only because every caller shares them.
- Use `utcNow()` rather than `datetime.utcnow()` (deprecated) or `datetime.now(timezone.utc)`. It is
  deliberately **naive**: `getTimeFrame` compares against datetimes parsed from the forecast's own
  date/time strings, which carry no tzinfo, and Python refuses to compare aware with naive.
- Open-Meteo responses are cached in `webapp/.cache.sqlite` for 1 hour via `requests_cache`. Delete it
  to force a refetch while debugging.
- Geocoding uses Nominatim with user agent `ESOWC-Meteogram-2018`; elevation comes from
  open-elevation.com and falls back to `-999`.
