// The start page: places to try, in the page's language, each a card with a
// three-day overview. A card is a link to the place's full meteogram.
import type { MouseEvent } from "react";

import type { Example, PictogramItem } from "../api/client";
import { useExamples, useForecast, useSchemes } from "../api/queries";
import { type I18n, useI18n } from "../i18n";
import { formatState, parseState } from "../state/urlState";
import { type Day, overview } from "./overview";

export const OVERVIEW_DAYS = 3;

export interface Selected {
  lat: number;
  lon: number;
  name: string;
}

/** The cards, or nothing at all while there are no places to suggest. */
export function Examples({ onSelect }: { onSelect: (place: Selected) => void }) {
  const i18n = useI18n();
  const examples = useExamples(i18n.lang);
  const schemes = useSchemes();
  const places = examples.data?.places ?? [];
  if (places.length === 0) return null;
  return (
    <section className="examples" aria-labelledby="examples-title">
      <h2 id="examples-title">{i18n.t.examples}</h2>
      <ul className="example-list">
        {places.map((place) => (
          <li key={`${place.lat},${place.lon}`}>
            <ExampleCard place={place} pictogramBase={schemes.data?.pictogram_base ?? null} onSelect={onSelect} />
          </li>
        ))}
      </ul>
      <p className="examples-note">{i18n.t.examplesNote}</p>
    </section>
  );
}

interface CardProps {
  place: Example;
  pictogramBase: string | null;
  onSelect: (place: Selected) => void;
}

function ExampleCard({ place, pictogramBase, onSelect }: CardProps) {
  const i18n = useI18n();
  // The automatic choice for three days, in 6-hour steps even where a model
  // offers hours: four symbols a day fit a card.
  const forecast = useForecast({
    lat: place.lat,
    lon: place.lon,
    product: null,
    days: OVERVIEW_DAYS,
    step_hours: 6,
    variant: "ensemble",
    name: null,
  });
  const result = forecast.data ? overview(forecast.data, new Date(), OVERVIEW_DAYS) : null;
  const selected = { lat: place.lat, lon: place.lon, name: place.name };

  const open = (event: MouseEvent<HTMLAnchorElement>) => {
    // A new tab or window is the browser's business.
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    onSelect(selected);
  };

  return (
    <article className="example" aria-busy={forecast.isPending} data-testid="example">
      <h3>
        <a href={hrefFor(selected)} onClick={open}>
          {place.name}
        </a>
        {result?.stale && <span className="example-stale"> · {i18n.t.outdated}</span>}
      </h3>
      {forecast.isError ? (
        <p className="example-missing">{i18n.t.noForecast}</p>
      ) : result && pictogramBase !== null ? (
        <div className="example-days">
          {result.days.map((day) => (
            <DayRow key={day.key} day={day} timeZone={result.timeZone} base={pictogramBase} i18n={i18n} />
          ))}
        </div>
      ) : (
        <div className="example-days placeholder" aria-hidden="true">
          {Array.from({ length: OVERVIEW_DAYS }, (_, i) => (
            <div key={i} className="example-day" />
          ))}
        </div>
      )}
    </article>
  );
}

function DayRow({ day, timeZone, base, i18n }: { day: Day; timeZone: string; base: string; i18n: I18n }) {
  const range = day.temperature;
  const low = range ? i18n.number(range[0]) : "";
  const high = range ? i18n.number(range[1]) : "";
  return (
    <div className="example-day" data-day={day.key}>
      <span className="example-weekday" title={i18n.dayMonth(day.start, timeZone)}>
        {day.today ? i18n.t.today : i18n.weekday(day.start, timeZone)}
      </span>
      <span className="example-glyphs" data-variable="cloud_cover">
        {day.slots.map((slot, i) => (
          <Glyph key={i} item={slot.cloud} variable="cloud_cover" past={slot.past} base={base} i18n={i18n} />
        ))}
      </span>
      <span className="example-glyphs" data-variable="precipitation">
        {day.slots.map((slot, i) => (
          <Glyph key={i} item={slot.rain} variable="precipitation" past={slot.past} base={base} i18n={i18n} />
        ))}
      </span>
      {range && (
        <span className="example-temperature" aria-label={i18n.t.temperatureRange(low, high)}>
          {low === high ? low : `${low}–${high}`} °C
        </span>
      )}
    </div>
  );
}

interface GlyphProps {
  item: PictogramItem | null;
  variable: string;
  past: boolean;
  base: string;
  i18n: I18n;
}

function Glyph({ item, variable, past, base, i18n }: GlyphProps) {
  if (!item) return <span className="example-glyph empty" />;
  const label = `${i18n.classLabel(variable, item.class)} (${i18n.t.levels[item.level] ?? item.level})`;
  return (
    <img
      className={past ? "example-glyph past" : "example-glyph"}
      src={base + item.pictogram}
      alt={label}
      title={label}
      width={26}
      height={26}
      data-level={item.level}
    />
  );
}

/** The place's own URL, so a card also opens in a new tab; keeps ?lang=. */
function hrefFor(place: Selected): string {
  const lang = new URLSearchParams(window.location.search).get("lang");
  const state = { ...parseState(""), ...place };
  return `${window.location.pathname}${formatState(state, lang ? { lang } : {})}`;
}
