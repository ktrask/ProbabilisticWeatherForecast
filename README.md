# Probabilistic Weather Forecast

Based on the project from the 2018 European Summer of Weather Code.

A meteogram that shows how certain a forecast is: the 51 members of the ECMWF
ensemble are reduced to quantiles, temperature is drawn as an uncertainty band,
and clouds, precipitation and wind as VSUP pictograms (Value-Suppressing
Uncertainty Palette) - the less the ensemble agrees, the vaguer the symbol.
Forecast data and place search come from [Open-Meteo](https://open-meteo.com/).

## Run it

```bash
cd webapp
docker build -t meteogram . && docker run -p 5003:5003 meteogram
# then open http://localhost:5003
```

Without network access, serve the recorded forecasts instead:

```bash
docker run -p 5003:5003 -e SOURCES_CONFIG=config/sources.fixtures.yaml meteogram
```

## Develop

```bash
cd webapp
pip install -r requirements-dev.txt
python -m api serve                         # API on 127.0.0.1:8000
cd frontend && npm ci && npm run dev        # UI on localhost:5173 (second terminal)

pytest                                      # backend tests, in webapp/
cd frontend && npm test && npm run e2e      # frontend unit and end-to-end tests
```

The pictogram rules are configuration, not code: `webapp/config/vsup.yaml`,
checked with `python -m vsup check`. See `CLAUDE.md` for the architecture and
`docs/neuentwicklung-plan.md` for the plan behind the rebuild (German).
