// Everything known about the step under the crosshair. Also a live region, so
// a screen reader announces the step as the arrow keys move through time.
import type { Forecast } from "../api/client";
import { useI18n } from "../i18n";
import { digitsFor, unitLabel } from "./format";
import { HOUR_MS } from "./time";

export const TOOLTIP_WIDTH = 230;
const ORDER = ["temperature_2m", "cloud_cover", "precipitation", "wind_speed_10m"];

export interface TooltipProps {
  forecast: Forecast;
  steps: Date[];
  index: number | null;
  // Where the box goes, in the chart's own pixels; each layout keeps it on screen.
  place: { left: number; top: number };
}

export function Tooltip({ forecast, steps, index, place }: TooltipProps) {
  const i18n = useI18n();
  if (index === null) return <div className="tooltip-region" role="status" aria-live="polite" />;
  const t = steps[index] as Date;
  const timeZone = forecast.location.timezone ?? "UTC";

  return (
    <div className="tooltip-region" role="status" aria-live="polite">
      <div className="tooltip" style={{ left: place.left, top: place.top, width: TOOLTIP_WIDTH }} data-testid="tooltip">
        <div className="when">
          {i18n.weekday(t, timeZone, "long")} {i18n.dayMonth(t, timeZone)}, {i18n.time(t, timeZone)}
        </div>
        <dl>
          {ORDER.map((variable) => {
            const series = forecast.variables[variable];
            if (!series) return null;
            const q = series.quantiles;
            const median = q.p50?.[index];
            if (median === undefined) return null;
            const digits = digitsFor(variable);
            const unit = unitLabel(series.unit);
            const low = q.p10?.[index];
            const high = q.p90?.[index];
            const item = forecast.pictograms[variable]?.items[index];
            let name = i18n.t.variables[variable] ?? variable;
            if (series.window_hours) {
              const until = new Date(t.getTime() + series.window_hours * HOUR_MS);
              name += ` ${i18n.time(t, timeZone)}–${i18n.time(until, timeZone)}`;
            }
            return (
              <div key={variable} className="row" data-variable={variable}>
                <dt>{name}</dt>
                <dd>
                  <strong>
                    {i18n.number(median, digits)} {unit}
                  </strong>
                  {low !== undefined && high !== undefined && (
                    <span className="spread">
                      {" "}
                      ({i18n.number(low, digits)}–{i18n.number(high, digits)})
                    </span>
                  )}
                  {item && (
                    <div className="class">
                      {i18n.classLabel(variable, item.class)} · {i18n.t.levels[item.level] ?? item.level}
                    </div>
                  )}
                </dd>
              </div>
            );
          })}
        </dl>
        <div className="note">
          {i18n.t.median}, {i18n.t.range} 10–90 %
        </div>
      </div>
    </div>
  );
}
