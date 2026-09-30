// The meteogram: cloud and precipitation pictograms, the temperature quantile
// band with daily extremes, wind pictograms, and a time axis in the location's
// local time. Hover, a tap or the arrow keys move a crosshair that lists
// everything known about the step.
//
// Two layouts: a row with time running to the right, which scrolls sideways
// when the steps do not fit (RowChart), and a column with time running down
// the page, for phones (ColumnChart). Both show the same window of steps.
import { type KeyboardEvent, useMemo, useState } from "react";

import type { Forecast } from "../api/client";
import { useI18n } from "../i18n";
import { ColumnChart } from "./ColumnChart";
import { type Window, dailyExtremes } from "./layout";
import { RowChart } from "./RowChart";
import { temperatureDomain } from "./shared";

export type Orientation = "row" | "column";

export interface MeteogramProps {
  forecast: Forecast;
  pictogramBase: string;
  window: Window;
  width: number;
  orientation?: Orientation;
}

export function Meteogram({ forecast, pictogramBase, window: view, width, orientation = "row" }: MeteogramProps) {
  const i18n = useI18n();
  const [active, setActive] = useState<number | null>(null);
  const timeZone = forecast.location.timezone ?? "UTC";
  const steps = useMemo(() => forecast.steps.map((s) => new Date(s)), [forecast.steps]);
  const { from, to } = view;

  const temperature = forecast.variables.temperature_2m;
  const domain = useMemo(() => (temperature ? temperatureDomain(temperature, from, to) : null), [temperature, from, to]);
  const extremes = useMemo(() => {
    const median = temperature?.quantiles.p50;
    return median ? dailyExtremes(steps, median, from, to, timeZone) : [];
  }, [temperature, steps, from, to, timeZone]);
  const place = forecast.location.name ?? `${forecast.location.lat}, ${forecast.location.lon}`;

  const onKey = (event: KeyboardEvent<HTMLDivElement>) => {
    const later = (c: number) => c + 1;
    const earlier = (c: number) => c - 1;
    const moves: Record<string, (current: number) => number> = {
      ArrowRight: later,
      ArrowLeft: earlier,
      ArrowDown: later,
      ArrowUp: earlier,
      Home: () => from,
      End: () => to - 1,
    };
    if (event.key === "Escape") {
      setActive(null);
      return;
    }
    const move = moves[event.key];
    if (!move) return;
    event.preventDefault();
    setActive((current) => Math.max(from, Math.min(to - 1, current === null ? from : move(current))));
  };

  const Chart = orientation === "column" ? ColumnChart : RowChart;
  return (
    <div
      className={`meteogram ${orientation}`}
      data-testid="meteogram"
      data-orientation={orientation}
      style={{ width }}
      tabIndex={0}
      role="group"
      aria-label={i18n.t.chartLabel(place)}
      onKeyDown={onKey}
      onBlur={() => setActive(null)}
    >
      <Chart
        forecast={forecast}
        steps={steps}
        from={from}
        to={to}
        width={width}
        pictogramBase={pictogramBase}
        timeZone={timeZone}
        domain={domain}
        extremes={extremes}
        active={active}
        onActive={setActive}
      />
    </div>
  );
}
