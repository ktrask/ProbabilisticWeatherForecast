// The meteogram turned on its side, for phones: time runs down the page, one
// step per CELL pixels, and the columns are - left to right - the time, clouds,
// precipitation, the temperature band and wind, in the order the row layout
// stacks them top to bottom. The page scrolls; the column heads stay on top.
import { scaleLinear, scaleUtc } from "d3-scale";
import { area, curveMonotoneY, line } from "d3-shape";
import { type PointerEvent, useEffect, useRef } from "react";

import { useI18n } from "../i18n";
import { unitLabel } from "./format";
import { dayBands } from "./layout";
import type { ChartProps } from "./RowChart";
import { AFTER, BANDS, BEFORE, type Lane, PICTOGRAM, lanes, pictogramTime } from "./shared";
import { HOUR_MS } from "./time";
import { TOOLTIP_WIDTH, Tooltip } from "./Tooltip";

const CELL = 40; // pixels per step
const AXIS_WIDTH = 64; // day names and hours
const HEAD = 40; // column names and temperature ticks
const RIGHT = 8;
const BAND = { min: 120, max: 480 }; // width of the temperature column
const TOOLTIP_HEIGHT = 230; // about; to keep it above the end of the chart

interface Column extends Lane {
  x: number;
}

export function ColumnChart(props: ChartProps) {
  const { forecast, steps, from, to, width, pictogramBase, timeZone, domain, extremes, active, onActive } = props;
  const i18n = useI18n();
  const count = Math.max(1, to - from);
  const pictogram = Math.min(PICTOGRAM.max, CELL - 4);
  const columnWidth = pictogram + 8;

  const before = lanes(forecast, BEFORE);
  const after = lanes(forecast, AFTER);
  const temperature = forecast.variables.temperature_2m;
  const fixed = AXIS_WIDTH + columnWidth * (before.length + after.length) + RIGHT;
  const bandWidth = temperature ? Math.max(BAND.min, Math.min(BAND.max, width - fixed)) : 0;
  const chartWidth = fixed + bandWidth;

  let cx = AXIS_WIDTH;
  const columns: Column[] = [];
  for (const lane of before) {
    columns.push({ ...lane, x: cx });
    cx += columnWidth;
  }
  const bandLeft = cx;
  cx += bandWidth;
  for (const lane of after) {
    columns.push({ ...lane, x: cx });
    cx += columnWidth;
  }

  const stepMs = forecast.step_hours * HOUR_MS;
  const start = new Date((steps[from] as Date).getTime() - stepMs / 2);
  const end = new Date((steps[to - 1] as Date).getTime() + stepMs / 2);
  const height = CELL * count;
  const y = scaleUtc().domain([start, end]).range([0, height]);
  const tx =
    temperature && domain
      ? scaleLinear().domain(domain).nice(4).range([bandLeft + 14, bandLeft + bandWidth - 14])
      : null;
  const ticks = tx ? tx.ticks(4) : [];
  const indices = Array.from({ length: to - from }, (_, k) => from + k);
  const bands = dayBands(start, end, timeZone);
  const py = (i: number) => y(steps[i] as Date);

  // Keyboard steps that leave the screen bring it along.
  const body = useRef<SVGSVGElement>(null);
  useEffect(() => {
    const svg = body.current;
    if (!svg || active === null) return;
    const box = svg.getBoundingClientRect();
    if (box.height === 0) return;
    const at = box.top + py(active);
    if (at < HEAD || at > window.innerHeight - CELL) window.scrollBy({ top: at - window.innerHeight / 2 });
  }, [active]); // py follows from the steps shown

  const pick = (event: PointerEvent<SVGRectElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    const t = y.invert(event.clientY - box.top).getTime();
    const index = from + Math.round((t - (steps[from] as Date).getTime()) / stepMs);
    onActive(Math.max(from, Math.min(to - 1, index)));
  };

  let tooltipTop = 0;
  if (active !== null) {
    const at = py(active);
    tooltipTop = at + 14 + TOOLTIP_HEIGHT > height ? Math.max(0, at - 14 - TOOLTIP_HEIGHT) : at + 14;
  }
  const precipitation = columns.find((c) => c.windowMs !== null);
  const q = temperature?.quantiles;
  const median = q?.p50;

  return (
    <div className="column-chart" style={{ width: chartWidth }}>
      <div className="column-head">
        <svg width={chartWidth} height={HEAD} aria-hidden="true">
          {columns.map((c) => (
            <text key={c.variable} x={c.x + columnWidth / 2} y={15} textAnchor="middle" className="column-name">
              {i18n.t.shortNames[c.variable] ?? c.variable}
            </text>
          ))}
          {tx && (
            <text x={bandLeft + bandWidth / 2} y={15} textAnchor="middle" className="column-name">
              {i18n.t.shortNames.temperature_2m}
            </text>
          )}
          {tx &&
            ticks.map((tick) => (
              <text key={tick} x={tx(tick)} y={33} textAnchor="middle" className="tick">
                {i18n.number(tick)}
                {tick === ticks.at(-1) ? ` ${unitLabel(temperature?.unit ?? "")}` : ""}
              </text>
            ))}
        </svg>
      </div>
      <div className="column-body">
        <svg width={chartWidth} height={height} ref={body}>
          <g className="days">
            {bands.map((band) => (
              <rect
                key={band.key}
                className={band.shaded ? "day shaded" : "day"}
                x={0}
                y={y(band.start)}
                width={chartWidth}
                height={Math.max(0, y(band.end) - y(band.start))}
              />
            ))}
            {bands.slice(1).map((band) => (
              <line key={band.key} className="midnight" x1={0} x2={chartWidth} y1={y(band.start)} y2={y(band.start)} />
            ))}
          </g>
          {tx && q && (
            <g className="temperature">
              {ticks.map((tick) => (
                <g key={tick} className="grid">
                  <line x1={tx(tick)} x2={tx(tick)} y1={0} y2={height} />
                </g>
              ))}
              {BANDS.filter(([lo, hi]) => q[lo] && q[hi]).map(([loName, hiName, className]) => {
                const loValues = q[loName] as number[];
                const hiValues = q[hiName] as number[];
                const path = area<number>()
                  .y(py)
                  .x0((i) => tx(loValues[i] as number))
                  .x1((i) => tx(hiValues[i] as number))
                  .curve(curveMonotoneY)(indices);
                return <path key={className} className={className} d={path ?? ""} />;
              })}
              {median && (
                <path
                  className="median"
                  d={line<number>().y(py).x((i) => tx(median[i] as number)).curve(curveMonotoneY)(indices) ?? ""}
                />
              )}
              {extremes.map((e) => (
                <g
                  key={`${e.kind}${e.index}`}
                  className={`extreme ${e.kind}`}
                  transform={`translate(${tx(e.value) + (e.kind === "max" ? 17 : -17)},${py(e.index)})`}
                >
                  <circle r={12} />
                  <text dy="0.35em" textAnchor="middle">
                    {i18n.number(e.value)}
                  </text>
                </g>
              ))}
            </g>
          )}
          {columns.map((column) => (
            <g key={column.variable} className="pictograms" data-variable={column.variable}>
              {indices.map((i) => {
                const item = column.series.items[i];
                const at = pictogramTime(steps[i] as Date, column.windowMs, end);
                if (!item || !at) return null;
                const label = `${i18n.classLabel(column.variable, item.class)} (${i18n.t.levels[item.level] ?? item.level})`;
                return (
                  <image
                    key={i}
                    href={pictogramBase + item.pictogram}
                    x={column.x + (columnWidth - pictogram) / 2}
                    y={y(at) - pictogram / 2}
                    width={pictogram}
                    height={pictogram}
                    data-level={item.level}
                  >
                    <title>{label}</title>
                  </image>
                );
              })}
            </g>
          ))}
          <g className="axis">
            {bands.map((band) => {
              if (y(band.end) - y(band.start) < 30) return null;
              const middle = new Date((band.start.getTime() + band.end.getTime()) / 2);
              const top = y(band.start);
              return (
                <text key={band.key} x={4} y={top + 16} className="day-label">
                  <tspan x={4}>{i18n.weekday(middle, timeZone, "short")}</tspan>
                  <tspan x={4} dy={14} className="day-date">
                    {i18n.dayMonth(middle, timeZone)}
                  </tspan>
                </text>
              );
            })}
            {indices.map((i) => (
              <g key={i} transform={`translate(0,${py(i)})`}>
                <line x1={AXIS_WIDTH - 5} x2={AXIS_WIDTH} />
                <text x={AXIS_WIDTH - 8} dy="0.32em" textAnchor="end" className="hour">
                  {i18n.time(steps[i] as Date, timeZone).slice(0, 2)}
                </text>
              </g>
            ))}
          </g>
          {active !== null && (
            <g className="crosshair" pointerEvents="none">
              {precipitation && (steps[active] as Date).getTime() + (precipitation.windowMs ?? 0) <= end.getTime() && (
                <rect
                  className="window"
                  x={precipitation.x}
                  y={py(active)}
                  width={columnWidth}
                  height={y(new Date((steps[active] as Date).getTime() + (precipitation.windowMs ?? 0))) - py(active)}
                />
              )}
              <line x1={AXIS_WIDTH} x2={chartWidth} y1={py(active)} y2={py(active)} />
            </g>
          )}
          <rect
            className="hover-target"
            x={AXIS_WIDTH}
            y={0}
            width={chartWidth - AXIS_WIDTH}
            height={height}
            onPointerDown={pick}
            onPointerMove={(e) => {
              if (e.pointerType === "mouse") pick(e);
            }}
            onPointerLeave={(e) => {
              if (e.pointerType === "mouse") onActive(null);
            }}
            onPointerCancel={() => onActive(null)}
          />
        </svg>
        <Tooltip
          forecast={forecast}
          steps={steps}
          index={active}
          place={{ left: Math.max(0, chartWidth - TOOLTIP_WIDTH - 4), top: tooltipTop }}
        />
      </div>
    </div>
  );
}
