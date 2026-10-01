import { type RefObject, useLayoutEffect, useMemo, useRef, useState } from "react";

import { type Forecast, type Product, type Schemes, ApiError } from "./api/client";
import { useForecast, useProducts, useSchemes } from "./api/queries";
import { useI18n } from "./i18n";
import { Legend } from "./legend/Legend";
import { daysAvailable, visibleWindow } from "./meteogram/layout";
import { Meteogram } from "./meteogram/Meteogram";
import { SearchBox } from "./search/SearchBox";
import { automaticChoice, covers, offered, stepFor } from "./state/products";
import { type Layout, type Navigate, type ViewState, useUrlState } from "./state/urlState";

export function App() {
  const i18n = useI18n();
  const [state, navigate] = useUrlState();
  const products = useProducts();
  const schemes = useSchemes();
  const list = products.data?.products;
  // No product in the URL: the API chooses the finest model for place and days.
  // The same rule here tells which model that will be, so that moving the days
  // fetches again only when the model changes.
  const auto = state.product === null;
  const expected =
    auto && list && state.lat !== null && state.lon !== null
      ? automaticChoice(list, state.lat, state.lon, state.days)?.id ?? null
      : null;
  const query = useMemo(
    () =>
      state.lat !== null && state.lon !== null && (!auto || list)
        ? {
            lat: state.lat,
            lon: state.lon,
            product: state.product,
            days: auto ? state.days : null,
            // Under the automatic choice the API settles the step; a model chosen by
            // hand is only asked for one it offers.
            step_hours: auto ? state.step : stepFor(list?.find((p) => p.id === state.product), state.step),
            variant: state.variant,
            name: state.name,
          }
        : null,
    // The days matter only through the expected model; see the key below.
    [state.lat, state.lon, state.product, auto ? expected : null, state.step, state.variant, state.name, list !== undefined],
  );
  const key = query && auto ? { ...query, days: null, expected } : query;
  const forecast = useForecast(query, key);

  return (
    <div className="app">
      <header>
        <div className="brand">
          <h1>{i18n.t.title}</h1>
          <p>{i18n.t.tagline}</p>
        </div>
        <SearchBox
          onSelect={(place) => {
            // A regional model chosen for the last place may not reach the new one.
            const chosen = products.data?.products.find((p) => p.id === state.product);
            navigate(chosen && !covers(chosen, place.lat, place.lon) ? { ...place, product: null } : place);
          }}
        />
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
            // At the edge of a rotated regional grid there may be no forecast after all.
            useDefault={state.product !== null && !products.data?.products.find((p) => p.id === state.product)?.default
              ? () => navigate({ product: null })
              : null}
          />
        ) : (
          <ForecastView
            forecast={forecast.data}
            updating={forecast.isPlaceholderData}
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

interface ErrorBoxProps {
  error: Error | null;
  retry: () => void;
  useDefault: (() => void) | null; // set while a model other than the default is chosen
}

function ErrorBox({ error, retry, useDefault }: ErrorBoxProps) {
  const i18n = useI18n();
  // A 4xx will fail the same way again; only offer a retry for the rest.
  const status = error instanceof ApiError ? error.status : null;
  const permanent = status !== null && status < 500;
  const otherModel = status === 404 && useDefault !== null;
  return (
    <div className="error" role="alert">
      <p>
        <strong>{otherModel ? i18n.t.modelNoData : status === 404 ? i18n.t.noData : i18n.t.errorTitle}</strong>
      </p>
      {error && <p className="detail">{error.message}</p>}
      {otherModel && (
        <button type="button" onClick={useDefault}>
          {i18n.t.useDefault}
        </button>
      )}
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
  updating: boolean; // another model is loading; this is the previous one
  schemes: Schemes;
  products: Product[];
  state: ViewState;
  navigate: Navigate;
}

function ForecastView({ forecast, updating, schemes, products, state, navigate }: ViewProps) {
  const i18n = useI18n();
  const [now] = useState(() => new Date());
  const steps = useMemo(() => forecast.steps.map((s) => new Date(s)), [forecast.steps]);
  const probe = visibleWindow(steps, forecast.step_hours, now, 1);
  const dataDays = daysAvailable(steps.length, probe.from, forecast.step_hours);
  const fallback = products.find((p) => p.default);
  // Chosen automatically, more days may bring another model: offer as many as the default reaches.
  const maxDays =
    state.product === null && fallback ? Math.max(dataDays, Math.ceil(fallback.horizon_days)) : dataDays;
  const days = Math.min(state.days, maxDays);
  const view = visibleWindow(steps, forecast.step_hours, now, days);
  const box = useRef<HTMLDivElement>(null);
  const width = useWidth(box);

  const drawn = new Set(Object.values(forecast.pictograms).map((p) => p.scheme));
  const legendSchemes = schemes.schemes.filter((s) => drawn.has(s.name));
  const product = products.find((p) => p.id === forecast.product) ?? fallback;
  const choices = offered(products, state.lat, state.lon, state.product);
  const wouldChoose =
    state.lat !== null && state.lon !== null ? automaticChoice(products, state.lat, state.lon, state.days) : undefined;
  const autoLabel = (forecast.automatic ? product : wouldChoose)?.label ?? "";
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
          {choices.length > 1 && (
            <label className="product">
              {i18n.t.product}{" "}
              <select
                value={state.product ?? ""}
                onChange={(e) => {
                  const next = e.target.value || null;
                  // A step the next model lacks is dropped; it shows its own.
                  const step = next === null ? state.step : stepFor(products.find((p) => p.id === next), state.step);
                  navigate({ product: next, step });
                }}
              >
                <option value="">{i18n.t.automatic(autoLabel)}</option>
                {choices.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.label} ({i18n.t.productDetails(p.members, i18n.number(p.grid_km), i18n.number(p.horizon_days, p.horizon_days % 1 ? 1 : 0))})
                  </option>
                ))}
              </select>
            </label>
          )}
          {product && product.steps.length > 1 && (
            <StepSwitch
              steps={product.steps}
              step={forecast.step_hours}
              onChange={(step) => navigate({ step }, { replace: true })}
            />
          )}
          <LayoutSwitch layout={state.layout} onChange={(layout) => navigate({ layout }, { replace: true })} />
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
      <div className={updating ? "chart updating" : "chart"} ref={box} aria-busy={updating}>
        {width > 0 && (
          <Meteogram
            forecast={forecast}
            pictogramBase={schemes.pictogram_base}
            window={view}
            width={width}
            orientation={state.layout}
          />
        )}
      </div>
      <p className="source">
        {/* Recorded quantiles do not know their members; the product says how many it runs. */}
        {i18n.t.source(
          forecast.automatic && product ? i18n.t.chosenAutomatically(product.label) : product?.label ?? forecast.run.source,
          forecast.run.members ?? product?.members ?? null,
        )}
      </p>
      <Legend schemes={legendSchemes} pictogramBase={schemes.pictogram_base} />
    </article>
  );
}

/** The step widths a model offers, the one drawn pressed: hourly or 6-hourly. */
function StepSwitch({ steps, step, onChange }: { steps: number[]; step: number; onChange: (step: number) => void }) {
  const i18n = useI18n();
  return (
    <div className="layout-switch step-switch" role="group" aria-label={i18n.t.stepLabel}>
      {[...steps].sort((a, b) => a - b).map((hours) => (
        <button key={hours} type="button" aria-pressed={hours === step} onClick={() => onChange(hours)}>
          {i18n.t.stepName(hours)}
        </button>
      ))}
    </div>
  );
}

/** Two buttons, one pressed: time to the right, or time down the page. */
function LayoutSwitch({ layout, onChange }: { layout: Layout; onChange: (layout: Layout) => void }) {
  const i18n = useI18n();
  const options: [Layout, string, string][] = [
    // Icons: three bars side by side over a time axis, or stacked beside one.
    ["row", i18n.t.horizontal, "M2 3h12M2 7h12M2 11h12M2 14.5h12"],
    ["column", i18n.t.vertical, "M3 2v12M7 2v12M11 2v12M14.5 2v12"],
  ];
  return (
    <div className="layout-switch" role="group" aria-label={i18n.t.layout}>
      {options.map(([value, label, icon]) => (
        <button key={value} type="button" aria-pressed={layout === value} onClick={() => onChange(value)}>
          <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
            <path d={icon} />
          </svg>
          {label}
        </button>
      ))}
    </div>
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
