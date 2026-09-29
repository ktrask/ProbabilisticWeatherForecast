# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Probabilistic weather meteograms, based on the 2018 ECMWF European Summer of Weather Code project.
The core idea is VSUP (Value-Suppressing Uncertainty Palette) encoding: ensemble spread is rendered
as pictograms whose visual specificity shrinks as forecast uncertainty grows, instead of plotting a
single deterministic line.

The app was rebuilt in 2026 (plan, in German: `docs/neuentwicklung-plan.md`): a Python backend that
returns only JSON, a browser-drawn meteogram, and VSUP rules as validated configuration. The old
Flask + matplotlib app was removed in phase 4; its pictogram rules survive as a frozen test
reference. Phases 0-4 are done; phase 5 (HRES, a second product) is open.

## Commands

Run from `webapp/` (Python 3.12, Node 22).

```bash
cd webapp
pip install -r requirements-dev.txt          # requirements.txt + pytest

python -m api serve                          # API dev server, 127.0.0.1:8000, docs at /api/docs
SOURCES_CONFIG=config/sources.fixtures.yaml python -m api serve   # offline: recorded forecasts
python -m vsup check                         # validate config/vsup.yaml + rule coverage on the fixtures
python -m sources check                      # validate config/sources.yaml against the schemes
python -m vsup schema -o config/vsup.schema.json        # after changing vsup/config.py's models
python -m sources schema -o config/sources.schema.json  # after changing sources/config.py's models
python -m api openapi -o api/openapi.json               # after changing a route or response model

cd frontend && npm ci
npm run dev                        # Vite on :5173, proxies /api and /pictograms to 127.0.0.1:8000
npm run build                      # tsc + vite build -> dist/ (the API serves it when present)
npm test                           # Vitest unit/component tests (jsdom)
npm run e2e                        # Playwright; starts the offline API and a preview build itself
npm run e2e -- --update-snapshots  # after an intended visual change - review the PNG diff
npm run gen:api                    # src/api/schema.d.ts from api/openapi.json; check:api verifies it
npx playwright install chromium    # once, for the pinned browser the screenshots are taken with

docker build -t meteogram . && docker run -p 5003:5003 meteogram
docker run -p 5003:5003 -e SOURCES_CONFIG=config/sources.fixtures.yaml meteogram   # offline demo
```

All Python CLIs must be run with `-m`: they are package modules and import their siblings by
package path. Environment: `HOST`/`PORT`, `WEB_CONCURRENCY`, `GUNICORN_TIMEOUT` (container only);
`VSUP_CONFIG`, `SOURCES_CONFIG`, `FRONTEND_DIST`, `FORECAST_CACHE_TTL_S`, `FORECAST_CACHE_SIZE`,
`GEOCODE_CACHE_TTL_S`, `GEOCODE_CACHE_SIZE`, `GEOCODER_TIMEOUT_S` (`api/settings.py`).

### Tests

```bash
pytest                             # everything, incl. the live Open-Meteo checks
pytest -m "not live"               # offline only - no network at all
pytest -m live                     # only the live checks; they skip when the network is down
pytest tests/test_vsup_golden.py   # a single file

python tests/generate_fixtures.py [key ...]   # re-record tests/fixtures/ from the live API
```

`tests/conftest.py` chdirs to `webapp/` at import, so pytest works from any directory. There is no
linter and no CI; `tsc --noEmit` (in `npm run build`) is the frontend's type check.

## Architecture

Everything lives under `webapp/`:

- **`core/`** — no HTTP, no rendering. `variables.py` lists each variable with its one canonical
  unit (`degC`, `mm`, `percent`, `m/s`) and whether it is an `instant` or a `sum`; `units.py`
  converts to those units or raises. `model.py` is the `Forecast` (Pydantic): steps in UTC, unit and
  aggregation per variable, quantiles named `p0`…`p100`. Its validators are the contract - a
  `Forecast` that exists has canonical units, evenly spaced steps and ordered, finite quantiles.
  `reduce.py` + `pipeline.py` turn a `SourceResult` into a `Forecast`. `configfile.py` is the YAML
  loading `vsup/` and `sources/` share. `legacy.py` reads the old `allMeteogramData` format, for the
  fixtures recorded in it.
- **`sources/`** — adapters returning a `SourceResult` in canonical units and UTC:
  `open_meteo.OpenMeteoEnsemble` (async httpx, flatbuffers), `fixture.FixtureSource` (the recorded
  forecasts), `geocode.OpenMeteoGeocoder`. `config.py` + **`config/sources.yaml`** define sources
  and the *products* the UI offers (a source plus the schemes drawn from it; the first is the
  default). `config/sources.fixtures.yaml` is its offline twin with the same product names.
- **`vsup/`** + **`config/vsup.yaml`** — the pictogram rules. `rules` mode is an ordered list of
  `when:` expressions (own parser in `expr.py`, never `eval`); `tree` mode is intensity classes
  merging into groups as certainty drops. `config.load()` collects *every* problem with its line
  number before raising `ConfigError`. `config/vsup.schema.json` is generated from the models.
- **`api/`** — FastAPI, GET only: `/api/forecast`, `/api/geocode`, `/api/products`, `/api/schemes`,
  `/api/health`, `/pictograms/<version>/…`, and the built frontend at `/`. `create_app()` is a
  factory, so importing it has no side effects; it loads and cross-checks both configs, so a broken
  one stops the start. `api/openapi.json` is the checked-in contract.
- **`frontend/`** — React 19 + TypeScript + Vite, TanStack Query, the chart as SVG with
  d3-scale/d3-shape (no chart library). `src/api/schema.d.ts` is generated from `api/openapi.json`,
  so an API change the frontend does not follow is a type error. `src/state/` keeps place, product,
  variant and days in the URL only; `src/meteogram/layout.ts` + `time.ts` are the pure logic.
- **`pictograms/`** — the glyphs, where `vsup.yaml`'s `pictogram_root` points. Ensemble SVGs, the
  PNGs the legacy rules drew, and the 48 HRES PNGs in `*/enhanced_hres/` (unused until phase 5
  redraws them; kept as the design reference).

Layering: `core` <- `sources`, `vsup` <- `api`. `sources.config.load()` checks products against a
loaded VSUP config handed to it; only the `python -m sources` CLI imports `vsup` itself.

### Units

Every variable has one canonical unit and every adapter delivers it. The adapter asks Open-Meteo for
canonical units outright (`wind_speed_unit=ms` - its default is km/h) and still reads the unit each
series reports, converting or refusing it: Open-Meteo silently ignores a parameter it does not know,
so asking is not proof. A scheme written in another unit declares it and is converted once, at load
time - which is only sound because every conversion in `core/units.py` is strictly increasing; keep
it so. History: cloud cover (fraction vs percent), precipitation (metres vs mm) and wind (km/h vs
m/s) each once reached thresholds in the wrong unit, and none of it failed - it just drew the wrong
pictograms. Do not restate the wind thresholds in km/h.

### Time and aggregation

- **Sum variables are totalled per member, then reduced to quantiles** - the 90th percentile of
  6-hour totals is not the sum of hourly 90th percentiles. `kind: sum` in `core/variables.py` is all
  a new accumulated variable needs.
- **The step at `t` covers `[t, t+6h)`** = hourly rows `t+1 … t+6`, because Open-Meteo reports
  precipitation as the *preceding* hour's total. The legacy pipeline summed `t … t+5`, an hour
  early; only the legacy sample in `tests/fixtures/legacy/` still carries that.
- The adapter asks for 15 days; the run ends around hour 350 and Open-Meteo pads the rest with NaN.
  `reduce.complete_rows` drops trailing NaN rows and refuses holes, and all variables share the
  steps a 6-hour *sum* can fill (58-59 for `ecmwf_ifs025`).
- Steps are UTC everywhere in the backend. The frontend works in the **forecast location's time
  zone, never the browser's** (`meteogram/time.ts` via `Intl`, DST included - the day the clocks go
  back is 25 hours wide). Playwright runs the browser in America/New_York to keep it so.

### VSUP schemes

- **The `*-legacy` schemes are frozen.** `tests/test_vsup_golden.py` holds them to exactly the
  pictograms of the old `getVSUP*Coordinate()` functions - kept verbatim in
  `tests/legacy_reference.py` - on every fixture step and on an exhaustive grid around every
  threshold (read from that source, not from the YAML). Do not edit either side; design changes go
  into the `*-vsup` tree schemes, which are allowed to differ.
- **"Certain" (level 3) means the middle two thirds of the members (p17..p83) lie in one class**
  - "likely" in the IPCC's wording; "likely" (level 2) means the middle half (p25..p75) in one
  group. The user's decision of 2026-09-29, after comparing on live data for the five places:
  80 % (p10..p90) made only the edge classes ever certain (clouds 9 %, rain 18 % of steps), 50 %
  was too weak a claim for "certain", 66 % gives 13 % / 26 % / wind 60 %. Middle classes (light
  rain, partly cloudy) are still almost never certain: that comes from their narrow ranges, not
  from the threshold - changing it would mean changing the classes. The legend explains each
  level from `/api/schemes` (`levels`), so it stays true if the config changes. p17/p83 are why
  `quantiles` has nine levels and why the legacy-format fixtures could not serve it.
- The legacy rules reference the old PNGs, whose rain glyphs have a white, non-transparent
  background that shows on shaded days. Only the legacy schemes use them.

### Start-up checks

`sources.config.load()` refuses a product whose scheme is missing, draws a variable the source does
not deliver, is written for a window other than the 6-hour step, reads `deterministic`, or needs
quantiles a recorded source lacks; two schemes for one variable are refused too. What would
otherwise fail on some request fails at start-up, with file and line.

### API behaviour

- **Errors:** 422 invalid parameter (an unknown `product` too, in FastAPI's error shape), 404
  `NoData` (no data for the place, e.g. no fixture near it), 409 variant not offered (`hres`
  everywhere for now), 502 `SourceError`/`DataGap`, 504 `SourceTimeout`. Bodies are `{"detail": ...}`.
- **Cache:** `api/cache.TTLCache`, in process, per worker. Forecasts 1 h keyed by source, variables
  and coordinates rounded to 2 decimals (the grid is 25 km); geocoding 1 day by normalised query.
  Concurrent identical requests share one upstream fetch; a caller that hangs up does not cancel
  it; failures are not cached.
- **Static files:** pictograms under the VSUP config's `version` hash with `immutable` caching - a
  changed file or config gets a new URL. The frontend (`FRONTEND_DIST`, default `frontend/dist`) is
  mounted after every route: `index.html` with `no-cache`, hashed `assets/*` immutable. Without a
  build the app is API-only, which is what `npm run dev` wants.
- **Geocoding is Open-Meteo, not Nominatim.** Nominatim's usage policy forbids search-as-you-type
  and anything above 1 request/s; the location box searches while typing.

### Frontend behaviour

- **Instants sit at their step, totals in the middle of their window**: cloud, wind and temperature
  at `t`, precipitation at `t + 3h`, between two instants. A total is drawn only if its whole window
  fits, so there is one precipitation pictogram fewer than steps.
- **Long meteograms split into sections one below the other** (`layout.sections`) when a step would
  get under 28 px (`MIN_CELL`), but never into sections shorter than 5 days (`MIN_SECTION_DAYS`,
  the user's call - splitting sooner read as too fragmented; on a phone the pictograms shrink
  instead up to 5 days). Cuts are at local midnight, as even as the days allow; neighbours share the
  cut step so the line runs on and the total starting there is drawn once, in the later section.
  All sections share one px-per-step and one temperature scale.
- **The view starts at the step nearest to now.** A forecast that ended before now is shown from its
  start with a "stale" note - which is what the offline fixtures look like once they age.
- **Coordinates in the URL are never rounded on the way back out** (`formatState`): rounding moved
  the place on the first adjustment and refetched the forecast. Whoever picks a place rounds it.
- A bare comma between two whole numbers is not a coordinate separator: "52,26" is German for 52.26.

## Tests

- **Fixtures** (`tests/fixtures/*.json`) cover five deliberately different climates (temperate,
  subarctic, equatorial, alpine, arid) so every pictogram branch is reachable offline;
  `locations.json` holds their metadata. They are `Forecast` JSON written by `generate_fixtures.py`
  (re-recorded 2026-09-29 with nine quantiles, when "certain" moved to p17..p83).
  `FixtureSource` also reads the legacy `allMeteogramData` format, told apart by content; one file
  in it is kept in `tests/fixtures/legacy/` so that reader and `tests/schema.py` stay tested. A
  source's `quantile_levels` are what every recording offers, and the start-up check refuses a
  config that asks for more. Re-recording changes the weather, so the frontend's screenshot
  baselines must be re-recorded with it (`npm run e2e -- --update-snapshots`).
- Tests derive dates from each fixture's own first step, never `now()`, so they stay deterministic
  as fixtures age; the e2e tests freeze the browser clock inside the recording.
- `tests/fixtures/open_meteo/reykjavik_3d.fb` is one raw flatbuffers response recorded with the
  adapter's parameters. `test_sources.py` replays it through `httpx.MockTransport` and checks the
  reduction against numpy on the raw members. Reykjavik because it rains there.
- Python: `test_core.py` (units, `Forecast` contract, reduction), `test_sources.py`,
  `test_vsup_expr.py`, `test_vsup_config.py` (one case per validator error, with line numbers),
  `test_vsup_golden.py` (legacy parity), `test_catalog.py` (sources.yaml start-up checks),
  `test_api.py` (routes, status codes, cache, frontend serving, read-only routes, OpenAPI
  contract), `test_data_format.py` (fixtures), `test_deployment.py` (container), `test_live_api.py`.
- Frontend: Vitest next to the code (`*.test.ts[x]`) and Playwright in `frontend/e2e/` (screenshots
  of all five fixtures and on a phone, hover, days without refetch, search with back/forward, 404,
  legend). Baselines are taken with Playwright's pinned Chromium against `sources.fixtures.yaml`.
- Known bugs are recorded as `strict=True` xfails, so they flip to a loud XPASS once fixed.
- The API tests call the app through `httpx.ASGITransport` (`tests/test_api.get`): Starlette 1.7
  deprecates its httpx-based `TestClient`.

## Serving

- The container runs `gunicorn -k uvicorn_worker.UvicornWorker "api.app:create_app()"` from
  `startup.sh`, which `exec`s so gunicorn is PID 1 and `docker stop` stops it (~0.3 s on SIGTERM).
  `uvicorn.workers` is deprecated; the `uvicorn-worker` package replaces it. Every worker loads both
  configs on boot, so a broken one makes gunicorn exit (code 3, `ConfigError` with file and line)
  instead of serving errors.
- The Dockerfile builds the frontend in a Node stage; the running image has no compiler (every
  requirement ships a CPython 3.12 manylinux wheel, checked with `pip download --only-binary`),
  runs as an unprivileged user on a read-only `/app`, and has a health check on `/api/health`.
  `test_deployment.py` pins it to what the app needs - including a `COPY` of wherever
  `pictogram_root` points. Built and run on 2026-09-29: both workers boot, the container turns
  healthy, and over HTTP it served the page, hashed assets, pictograms (SVG and PNG), a live
  forecast (0.33 s cold, 5 ms cached), geocoding and a deep health check, refused bad parameters
  (422/409), POST (405) and path traversal (404), and rendered in a browser without a failed
  request. Claude has no Docker access on the development machine; the user runs `docker`.
- **`COPY` keeps the checkout's permissions.** This checkout has `startup.sh` and `pictograms/`
  unreadable for others, so the unprivileged user could not read them and the container exited at
  once (the old image hid this behind a `chown` to the app user). `chmod -R a+rX /app` before
  `USER` fixes it for any umask; keep it after the last `COPY`, which a test checks.
- Every route is a read-only GET and there are no sessions, so there is no CSRF protection and no
  `SECRET_KEY`. `test_api.TestContract` fails if a route accepts anything else - that is the signal
  to revisit it. Never commit a secret; the old Flask app's hardcoded key is still in the history.
- The development servers bind to 127.0.0.1 unless told otherwise.

## Conventions

- Every outbound call has an explicit timeout: `sources/http.get` passes the adapter's
  `httpx.Timeout` on each request, so a shared client configured without one cannot undo it.
- No import side effects: configs are loaded in `create_app()`, not at import.
- Pictograms are found through `pictogram_root` in `vsup.yaml`; never a second copy of the assets.
- TypeScript is pinned to 5.9 because openapi-typescript 7 requires `typescript ^5`.

## Known gaps

- **No HRES yet.** `deterministic` parses in `when:` expressions and the model has a slot for it,
  but no adapter delivers one, the `anchored` mode for HRES schemes does not exist, and a product
  cannot name a deterministic source. `variant=hres` answers 409. (The old app's HRES view needed a
  grib pipeline that commit `cc18d28` removed.)
- The API has no rate limiting, and its cache is per worker process.
- Frontend MVP gaps: no 12-hour aggregation (the plan's `step_hours` parameter - sections took its
  place for now), no SVG/PNG export, light theme only, and the variant/product pickers only appear
  once there is more than one to choose.
- A plotly rewrite of the old renderer was started twice and dropped (commit `3e00557`).
