// The meteogram as one SVG: cloud and precipitation pictograms, the
// temperature quantile band with daily extremes, wind pictograms, and a time
// axis in the location's local time. Hover, touch or the arrow keys move a
// crosshair that lists everything known about the step.
//
// Instant variables (clouds, wind, temperature) belong to their step's time.
// Precipitation is the total over [t, t + window), so its pictogram sits in
// the middle of that window - between two instants, like the precipitation
// bars of a classic meteogram.
import { scaleLinear, scaleUtc } from "d3-scale";
import { area, curveMonotoneX, line } from "d3-shape";
import { type KeyboardEvent, type PointerEvent, useMemo, useState } from "react";

import type { Forecast, PictogramSeries, VariableSeries } from "../api/client";
import { useI18n } from "../i18n";
import { unitLabel } from "./format";
import { type Window, dailyExtremes, dayBands, extent } from "./layout";
import { HOUR_MS } from "./time";
import { Tooltip } from "./Tooltip";

const ROW = { pictogram: 56, temperature: 210, axis: 46 };
const MARGIN = { left: 44, right: 14, top: 6 };
const PICTOGRAM = { min: 14, max: 46 };
// The temperature band, from the widest spread to the narrowest.
const BANDS: [string, string, string][] = [
  ["p0", "p100", "band-outer"],
  ["p10", "p90", "band-middle"],
  ["p25", "p75", "band-inner"],
];
// Pictogram rows above and below the temperature band, top to bottom.
const ABOVE = ["cloud_cover", "precipitation"];
const BELOW = ["wind_speed_10m"];

export interface MeteogramProps {
  forecast: Forecast;
  pictogramBase: string;
  window: Window;
  width: number;
}

interface Row {
  variable: string;
  series: PictogramSeries;
  y: number;
  windowMs: number | null; // for totals: the window each pictogram covers
}

export function Meteogram({ forecast, pictogramBase, window: view, width }: MeteogramProps) {
  const i18n = useI18n();
  const [active, setActive] = useState<number | null>(null);
  const timeZone = forecast.location.timezone ?? "UTC";
  const stepMs = forecast.step_hours * HOUR_MS;
  const steps = useMemo(() => forecast.steps.map((s) => new Date(s)), [forecast.steps]);
  const { from, to } = view;
  const first = steps[from] as Date;
  const last = steps[to - 1] as Date;
  const start = new Date(first.getTime() - stepMs / 2);
  const end = new Date(last.getTime() + stepMs / 2);
  const x = scaleUtc().domain([start, end]).range([MARGIN.left, width - MARGIN.right]);
  const cell = (width - MARGIN.left - MARGIN.right) / (to - from);
  const size = Math.max(PICTOGRAM.min, Math.min(PICTOGRAM.max, cell * 0.92));

  // Rows, top to bottom.
  let y = MARGIN.top;
  const rows: Row[] = [];
  const addRows = (variables: string[]) => {
    for (const variable of variables) {
      const series = forecast.pictograms[variable];
      if (!series) continue;
      const hours = forecast.variables[variable]?.window_hours;
      rows.push({ variable, series, y, windowMs: hours ? hours * HOUR_MS : null });
      y += ROW.pictogram;
    }
  };
  addRows(ABOVE);
  const temperature = forecast.variables.temperature_2m;
  const bandTop = y;
  if (temperature) y += ROW.temperature;
  addRows(BELOW);
  const axisTop = y;
  const height = axisTop + ROW.axis;

  const indices = useMemo(() => Array.from({ length: to - from }, (_, k) => from + k), [from, to]);
  const bands = useMemo(() => dayBands(start, end, timeZone), [start.getTime(), end.getTime(), timeZone]);

  const place = forecast.location.name ?? `${forecast.location.lat}, ${forecast.location.lon}`;

  const nearest = (px: number) => {
    const t = x.invert(px).getTime();
    const index = Math.round((t - (steps[0] as Date).getTime()) / stepMs);
    return Math.max(from, Math.min(to - 1, index));
  };
  const onPointer = (event: PointerEvent<SVGRectElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    setActive(nearest(event.clientX - box.left + MARGIN.left));
  };
  const onKey = (event: KeyboardEvent<SVGSVGElement>) => {
    const moves: Record<string, (current: number) => number> = {
      ArrowRight: (c) => c + 1,
      ArrowLeft: (c) => c - 1,
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

  return (
    <div className="meteogram" data-testid="meteogram" style={{ width }}>
      <svg
        width={width}
        height={height}
        tabIndex={0}
        role="group"
        aria-label={i18n.t.chartLabel(place)}
        onKeyDown={onKey}
        onBlur={() => setActive(null)}
      >
        <DayShading bands={bands} x={x} top={MARGIN.top} bottom={axisTop} />
        {temperature && (
          <TemperatureBand
            series={temperature}
            steps={steps}
            indices={indices}
            x={x}
            top={bandTop}
            height={ROW.temperature}
            left={MARGIN.left}
            right={width - MARGIN.right}
            timeZone={timeZone}
          />
        )}
        {rows.map((row) => (
          <PictogramRow
            key={row.variable}
            row={row}
            steps={steps}
            indices={indices}
            x={x}
            end={end}
            size={size}
            base={pictogramBase}
          />
        ))}
        <TimeAxis bands={bands} steps={steps} indices={indices} x={x} top={axisTop} timeZone={timeZone} cell={cell} />
        {active !== null && (
          <Crosshair
            x={x}
            at={steps[active] as Date}
            top={MARGIN.top}
            bottom={axisTop}
            windowRow={rows.find((r) => r.windowMs !== null)}
            windowEnd={end}
          />
        )}
        <rect
          className="hover-target"
          x={MARGIN.left}
          y={0}
          width={Math.max(0, width - MARGIN.left - MARGIN.right)}
          height={axisTop}
          onPointerMove={onPointer}
          onPointerDown={onPointer}
          onPointerLeave={() => setActive(null)}
        />
      </svg>
      <Tooltip forecast={forecast} steps={steps} index={active} x={active === null ? 0 : x(steps[active] as Date)} width={width} />
    </div>
  );
}

type Scale = ReturnType<typeof scaleUtc<number, number>>;

function DayShading({ bands, x, top, bottom }: { bands: ReturnType<typeof dayBands>; x: Scale; top: number; bottom: number }) {
  return (
    <g className="days">
      {bands.map((band) => (
        <rect
          key={band.key}
          className={band.shaded ? "day shaded" : "day"}
          x={x(band.start)}
          y={top}
          width={Math.max(0, x(band.end) - x(band.start))}
          height={bottom - top}
        />
      ))}
      {bands.slice(1).map((band) => (
        <line key={band.key} className="midnight" x1={x(band.start)} x2={x(band.start)} y1={top} y2={bottom} />
      ))}
    </g>
  );
}

interface BandProps {
  series: VariableSeries;
  steps: Date[];
  indices: number[];
  x: Scale;
  top: number;
  height: number;
  left: number;
  right: number;
  timeZone: string;
}

function TemperatureBand({ series, steps, indices, x, top, height, left, right, timeZone }: BandProps) {
  const i18n = useI18n();
  const q = series.quantiles;
  const from = indices[0] as number;
  const to = (indices[indices.length - 1] as number) + 1;
  const present = BANDS.filter(([lo, hi]) => q[lo] && q[hi]);
  const outer = present[0];
  const [lo, hi] = extent(outer ? [q[outer[0]] as number[], q[outer[1]] as number[]] : [q.p50 ?? []], from, to);
  // Room above and below for the extreme markers.
  const y = scaleLinear().domain([lo - 1.5, hi + 1.5]).nice(4).range([top + height - 14, top + 14]);
  const px = (i: number) => x(steps[i] as Date);
  const median = q.p50;
  const extremes = median ? dailyExtremes(steps, median, from, to, timeZone) : [];

  return (
    <g className="temperature">
      {y.ticks(4).map((tick) => (
        <g key={tick} className="grid">
          <line x1={left} x2={right} y1={y(tick)} y2={y(tick)} />
          <text x={left - 6} y={y(tick)} dy="0.32em" textAnchor="end">
            {i18n.number(tick)}
            {tick === y.ticks(4).at(-1) ? ` ${unitLabel(series.unit)}` : ""}
          </text>
        </g>
      ))}
      {present.map(([loName, hiName, className]) => {
        const loValues = q[loName] as number[];
        const hiValues = q[hiName] as number[];
        const path = area<number>()
          .x(px)
          .y0((i) => y(loValues[i] as number))
          .y1((i) => y(hiValues[i] as number))
          .curve(curveMonotoneX)(indices);
        return <path key={className} className={className} d={path ?? ""} />;
      })}
      {median && (
        <path className="median" d={line<number>().x(px).y((i) => y(median[i] as number)).curve(curveMonotoneX)(indices) ?? ""} />
      )}
      {extremes.map((e) => {
        const cy = y(e.value) + (e.kind === "max" ? -17 : 17);
        return (
          <g key={`${e.kind}${e.index}`} className={`extreme ${e.kind}`} transform={`translate(${px(e.index)},${cy})`}>
            <circle r={12} />
            <text dy="0.35em" textAnchor="middle">
              {i18n.number(e.value)}
            </text>
          </g>
        );
      })}
    </g>
  );
}

interface RowProps {
  row: Row;
  steps: Date[];
  indices: number[];
  x: Scale;
  end: Date;
  size: number;
  base: string;
}

function PictogramRow({ row, steps, indices, x, end, size, base }: RowProps) {
  const i18n = useI18n();
  const cy = row.y + ROW.pictogram / 2;
  return (
    <g className="pictograms" data-variable={row.variable}>
      {indices.map((i) => {
        const item = row.series.items[i];
        const t = steps[i] as Date;
        if (!item) return null;
        let center = t.getTime();
        if (row.windowMs !== null) {
          // A total is drawn only if its whole window is on the chart.
          if (t.getTime() + row.windowMs > end.getTime()) return null;
          center += row.windowMs / 2;
        }
        const cx = x(new Date(center));
        const label = `${i18n.classLabel(row.variable, item.class)} (${i18n.t.levels[item.level] ?? item.level})`;
        return (
          <image
            key={i}
            href={base + item.pictogram}
            x={cx - size / 2}
            y={cy - size / 2}
            width={size}
            height={size}
            data-level={item.level}
          >
            <title>{label}</title>
          </image>
        );
      })}
    </g>
  );
}

interface AxisProps {
  bands: ReturnType<typeof dayBands>;
  steps: Date[];
  indices: number[];
  x: Scale;
  top: number;
  timeZone: string;
  cell: number; // pixels per step
}

// Below this many pixels per step, hour labels would collide: then only noon
// is labelled, and on narrower cells none - the day labels carry the time.
const HOUR_LABEL_PX = 24;

function TimeAxis({ bands, steps, indices, x, top, timeZone, cell }: AxisProps) {
  const i18n = useI18n();
  return (
    <g className="axis">
      {indices.map((i) => {
        const t = steps[i] as Date;
        const hour = i18n.time(t, timeZone).slice(0, 2);
        const labelled = cell >= HOUR_LABEL_PX || (cell * 2 >= HOUR_LABEL_PX && hour === "12");
        return (
          <g key={i} transform={`translate(${x(t)},${top})`}>
            <line y1={0} y2={5} />
            {labelled && (
              <text y={16} textAnchor="middle" className="hour">
                {hour}
              </text>
            )}
          </g>
        );
      })}
      {bands.map((band) => {
        const width = x(band.end) - x(band.start);
        if (width < 34) return null;
        const middle = new Date((band.start.getTime() + band.end.getTime()) / 2);
        const label =
          width > 90
            ? `${i18n.weekday(middle, timeZone, "short")} ${i18n.dayMonth(middle, timeZone)}`
            : i18n.weekday(middle, timeZone, "short");
        return (
          <text key={band.key} x={x(middle)} y={top + 36} textAnchor="middle" className="day-label">
            {label}
          </text>
        );
      })}
    </g>
  );
}

interface CrosshairProps {
  x: Scale;
  at: Date;
  top: number;
  bottom: number;
  windowRow: Row | undefined;
  windowEnd: Date;
}

function Crosshair({ x, at, top, bottom, windowRow, windowEnd }: CrosshairProps) {
  const cx = x(at);
  const windowMs = windowRow?.windowMs ?? null;
  const showWindow = windowRow && windowMs !== null && at.getTime() + windowMs <= windowEnd.getTime();
  return (
    <g className="crosshair" pointerEvents="none">
      {showWindow && (
        <rect
          className="window"
          x={cx}
          y={windowRow.y}
          width={x(new Date(at.getTime() + windowMs)) - cx}
          height={ROW.pictogram}
        />
      )}
      <line x1={cx} x2={cx} y1={top} y2={bottom} />
    </g>
  );
}
