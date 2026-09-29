import { type RefObject, useLayoutEffect, useMemo, useRef, useState } from "react";

import { type Forecast, type Product, type Schemes, ApiError } from "./api/client";
import { useForecast, useProducts, useSchemes } from "./api/queries";
import { useI18n } from "./i18n";
import { Legend } from "./legend/Legend";
import { daysAvailable, visibleWindow } from "./meteogram/layout";
import { Meteogram } from "./meteogram/Meteogram";
import { SearchBox } from "./search/SearchBox";
import { type Navigate, type ViewState, useUrlState } from "./state/urlState";

export function App() {
  const i18n = useI18n();
  const [state, navigate] = useUrlState();
  const products = useProducts();
  const schemes = useSchemes();
  const query = useMemo(
    () =>
      state.lat !== null && state.lon !== null
        ? { lat: state.lat, lon: state.lon, product: state.product, variant: state.variant, name: state.name }
        : null,
    [state.lat, state.lon, state.product, state.variant, state.name],
  );
  const forecast = useForecast(query);

  return (
    <div className="app">
      <header>
        <div className="brand">
          <h1>{i18n.t.title}</h1>
          <p>{i18n.t.tagline}</p>
        </div>
        <SearchBox onSelect={(place) => navigate(place)} />
      </header>
      <main>
        {query === null ? (
          <p className="intro">{i18n.t.intro}</p>
        ) : forecast.isPending || schemes.isPending ? (
          <p className="status" aria-busy="true">
            {i18n.t.loading}
          </p>
        ) : forecast.isError || schemes.isError ? (
          <ErrorBox
            error={forecast.error ?? schemes.error}
            retry={() => {
              void forecast.refetch();
              void schemes.refetch();
            }}
          />
        ) : (
          <ForecastView
            forecast={forecast.data}
            schemes={schemes.data}
            products={products.data?.products ?? []}
            state={state}
            navigate={navigate}
          />
        )}
      </main>
    </div>
  );
}

function ErrorBox({ error, retry }: { error: Error | null; retry: () => void }) {
  const i18n = useI18n();
  // A 4xx will fail the same way again; only offer a retry for the rest.
  const status = error instanceof ApiError ? error.status : null;
  const permanent = status !== null && status < 500;
  return (
    <div className="error" role="alert">
      <p>
        <strong>{status === 404 ? i18n.t.noData : i18n.t.errorTitle}</strong>
      </p>
      {error && <p className="detail">{error.message}</p>}
      {!permanent && (
        <button type="button" onClick={retry}>
          {i18n.t.retry}
        </button>
      )}
    </div>
  );
}

interface ViewProps {
  forecast: Forecast;
  schemes: Schemes;
  products: Product[];
  state: ViewState;
  navigate: Navigate;
}

function ForecastView({ forecast, schemes, products, state, navigate }: ViewProps) {
  const i18n = useI18n();
  const [now] = useState(() => new Date());
  const steps = useMemo(() => forecast.steps.map((s) => new Date(s)), [forecast.steps]);
  const probe = visibleWindow(steps, forecast.step_hours, now, 1);
  const maxDays = daysAvailable(steps.length, probe.from, forecast.step_hours);
  const days = Math.min(state.days, maxDays);
  const view = visibleWindow(steps, forecast.step_hours, now, days);
  const box = useRef<HTMLDivElement>(null);
  const width = useWidth(box);

  const drawn = new Set(Object.values(forecast.pictograms).map((p) => p.scheme));
  const legendSchemes = schemes.schemes.filter((s) => drawn.has(s.name));
  const product = products.find((p) => p.id === state.product) ?? products.find((p) => p.default);
  const location = forecast.location;
  const title = location.name ?? `${location.lat}, ${location.lon}`;

  return (
    <article className="forecast">
      <div className="forecast-head">
        <div>
          <h2 data-testid="place">{title}</h2>
          <p className="where">
            {i18n.number(location.lat, 2)}°, {i18n.number(location.lon, 2)}°
            {location.elevation_m !== null && location.elevation_m !== undefined
              ? ` · ${i18n.number(location.elevation_m)} m`
              : ""}
            {location.timezone ? ` · ${location.timezone}` : ""}
          </p>
        </div>
        <div className="controls">
          {products.length > 1 && (
            <label>
              {i18n.t.product}{" "}
              <select value={product?.id ?? ""} onChange={(e) => navigate({ product: e.target.value })}>
                {products.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.label}
                  </option>
                ))}
              </select>
            </label>
          )}
          <label className="days">
            {i18n.t.days}{" "}
            <input
              type="range"
              min={1}
              max={maxDays}
              value={days}
              onChange={(e) => navigate({ days: Number(e.target.value) }, { replace: true })}
            />{" "}
            <output>{days}</output>
          </label>
        </div>
      </div>
      {view.stale && (
        <p className="stale" role="note">
          {i18n.t.stale}
        </p>
      )}
      <div className="chart" ref={box}>
        {width > 0 && (
          <Meteogram forecast={forecast} pictogramBase={schemes.pictogram_base} window={view} width={width} />
        )}
      </div>
      <p className="source">{i18n.t.source(product?.label ?? forecast.run.source, forecast.run.members ?? null)}</p>
      <Legend schemes={legendSchemes} pictogramBase={schemes.pictogram_base} />
    </article>
  );
}

/** The element's content width, following resizes. */
function useWidth(ref: RefObject<HTMLElement | null>): number {
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) return;
    const measure = () => setWidth(Math.floor(element.getBoundingClientRect().width));
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);
  return width;
}
