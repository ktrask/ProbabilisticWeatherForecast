// The meteogram in one row, time running left to right: cloud and precipitation
// pictograms, the temperature band with daily extremes, wind pictograms, and a
// time axis in the location's local time.
//
// Where the steps do not fit at MIN_CELL pixels each - many days, or a phone -
// the row does not break into pieces but scrolls sideways: by swiping, by
// trackpad, or with the indicator below it, a map of the days that shows which
// part is in view. The temperature axis stays where it is.
import { scaleLinear, scaleUtc } from "d3-scale";
import { area, curveMonotoneX, line } from "d3-shape";
import { type PointerEvent, type RefObject, useEffect, useLayoutEffect, useRef, useState } from "react";

import type { Forecast, VariableSeries } from "../api/client";
import { useI18n } from "../i18n";
import { unitLabel } from "./format";
import { type Extreme, dayBands } from "./layout";
import { ScrollIndicator } from "./ScrollIndicator";
import { AFTER, BANDS, BEFORE, type Lane, PICTOGRAM, lanes, pictogramTime } from "./shared";
import { HOUR_MS } from "./time";
import { TOOLTIP_WIDTH, Tooltip } from "./Tooltip";

export const MARGIN = { left: 44, right: 14, top: 4 };
const AXIS_HEIGHT = 44;
// Below this many pixels per step the pictograms get too small to tell their
// levels apart; the row then scrolls instead of shrinking further. At 24 px
// three days still fit on a phone, and every hour keeps its label.
export const MIN_CELL = 24;
// Below this many pixels per step, hour labels would collide: then only noon
// is labelled, and on narrower cells none - the day labels carry the time.
const HOUR_LABEL_PX = 24;

export interface ChartProps {
  forecast: Forecast;
  steps: Date[];
  from: number;
  to: number;
  width: number;
  pictogramBase: string;
  timeZone: string;
  domain: [number, number] | null;
  extremes: Extreme[];
  active: number | null;
  onActive: (index: number | null) => void;
}

interface Row extends Lane {
  y: number;
}

type Scale = ReturnType<typeof scaleUtc<number, number>>;
type Linear = ReturnType<typeof scaleLinear<number, number>>;

export function RowChart(props: ChartProps) {
  const { forecast, steps, from, to, width, pictogramBase, timeZone, domain, extremes, active, onActive } = props;
  const plot = Math.max(1, width - MARGIN.left - MARGIN.right);
  const count = Math.max(1, to - from);
  const cell = Math.max(MIN_CELL, plot / count);
  const inner = cell * count; // the whole row, of which `plot` pixels are in view
  const scrolls = inner > plot + 0.5;
  const pictogram = Math.max(PICTOGRAM.min, Math.min(PICTOGRAM.max, cell * 0.92));
  const rowHeight = Math.round(pictogram + 10);
  const bandHeight = Math.round(Math.max(130, Math.min(210, plot * 0.32)));

  const stepMs = forecast.step_hours * HOUR_MS;
  const start = new Date((steps[from] as Date).getTime() - stepMs / 2);
  const end = new Date((steps[to - 1] as Date).getTime() + stepMs / 2);
  const x = scaleUtc().domain([start, end]).range([0, inner]);

  let y = MARGIN.top;
  const rows: Row[] = [];
  for (const lane of lanes(forecast, BEFORE)) {
    rows.push({ ...lane, y });
    y += rowHeight;
  }
  const temperature = forecast.variables.temperature_2m;
  const bandTop = y;
  if (temperature) y += bandHeight;
  for (const lane of lanes(forecast, AFTER)) {
    rows.push({ ...lane, y });
    y += rowHeight;
  }
  const axisTop = y;
  const height = axisTop + AXIS_HEIGHT;
  const ty =
    temperature && domain
      ? scaleLinear().domain(domain).nice(4).range([bandTop + bandHeight - 14, bandTop + 14])
      : null;

  const indices = Array.from({ length: to - from }, (_, k) => from + k);
  const bands = dayBands(start, end, timeZone);

  const scroller = useRef<HTMLDivElement>(null);
  const view = useScrollView(scroller, inner);

  // Keyboard steps that leave the visible part bring it along.
  useEffect(() => {
    const element = scroller.current;
    if (!element || active === null || !scrolls || element.clientWidth === 0) return;
    const cx = x(steps[active] as Date);
    if (cx < element.scrollLeft || cx > element.scrollLeft + element.clientWidth) {
      element.scrollLeft = cx - element.clientWidth / 2;
    }
  }, [active, scrolls, inner, from]); // x follows from these

  const pick = (event: PointerEvent<SVGRectElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    const t = x.invert(event.clientX - box.left).getTime();
    const index = from + Math.round((t - (steps[from] as Date).getTime()) / stepMs);
    onActive(Math.max(from, Math.min(to - 1, index)));
  };

  // The tooltip stays inside the part of the row that is in view.
  const shownLeft = scrolls ? view.left : 0;
  const shownWidth = scrolls && view.visible > 0 ? view.visible : inner;
  let tooltipLeft = 0;
  if (active !== null) {
    const cx = x(steps[active] as Date);
    tooltipLeft = cx + 14 + TOOLTIP_WIDTH > shownLeft + shownWidth ? cx - 14 - TOOLTIP_WIDTH : cx + 14;
    tooltipLeft = Math.max(shownLeft, Math.min(tooltipLeft, shownLeft + shownWidth - TOOLTIP_WIDTH));
    tooltipLeft = Math.max(0, tooltipLeft);
  }

  return (
    <div className="row-chart">
      <div className="plot">
        {ty && <TemperatureTicks y={ty} unit={temperature?.unit ?? ""} height={height} />}
        <div
          className={scrolls ? "scroller scrolls" : "scroller"}
          ref={scroller}
          style={{ marginLeft: MARGIN.left, marginRight: MARGIN.right }}
          data-testid="scroller"
        >
          <div className="scroll-content" style={{ width: inner }}>
            <svg width={inner} height={height}>
              <DayShading bands={bands} x={x} top={MARGIN.top} bottom={axisTop} />
              {temperature && ty && (
                <TemperatureBand
                  series={temperature}
                  steps={steps}
                  indices={indices}
                  x={x}
                  y={ty}
                  extremes={extremes}
                  right={inner}
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
                  pictogram={pictogram}
                  rowHeight={rowHeight}
                  base={pictogramBase}
                />
              ))}
              <TimeAxis
                bands={bands}
                steps={steps}
                indices={indices}
                x={x}
                top={axisTop}
                timeZone={timeZone}
                cell={cell}
                stepHours={forecast.step_hours}
              />
              {active !== null && (
                <Crosshair
                  x={x}
                  at={steps[active] as Date}
                  top={MARGIN.top}
                  bottom={axisTop}
                  windowRow={rows.find((r) => r.windowMs !== null)}
                  windowEnd={end}
                  rowHeight={rowHeight}
                />
              )}
              <rect
                className="hover-target"
                x={0}
                y={0}
                width={inner}
                height={axisTop}
                // A scrolling row leaves sideways swipes to the scroller; one
                // that fits lets a finger drag the crosshair instead.
                style={{ touchAction: scrolls ? "pan-x pan-y" : "pan-y" }}
                onPointerDown={pick}
                onPointerMove={(e) => {
                  if (e.pointerType === "mouse" || (!scrolls && e.buttons)) pick(e);
                }}
                onPointerLeave={(e) => {
                  // A tapped step stays until the next tap or a tap elsewhere.
                  if (e.pointerType === "mouse") onActive(null);
                }}
                onPointerCancel={() => onActive(null)}
              />
            </svg>
            <Tooltip forecast={forecast} steps={steps} index={active} place={{ left: tooltipLeft, top: 8 }} />
          </div>
        </div>
        {scrolls && (
          <>
            <div className="fade start" style={{ left: MARGIN.left }} data-shown={view.left > 1} aria-hidden="true" />
            <div
              className="fade end"
              style={{ right: MARGIN.right }}
              data-shown={view.left + view.visible < inner - 1}
              aria-hidden="true"
            />
          </>
        )}
      </div>
      {scrolls && (
        <ScrollIndicator
          scroller={scroller}
          left={view.left}
          visible={view.visible}
          total={inner}
          bands={bands}
          x={x}
          timeZone={timeZone}
          style={{ marginLeft: MARGIN.left, marginRight: MARGIN.right }}
        />
      )}
    </div>
  );
}

/** How far `ref` is scrolled and how much of it is in view, following scrolls
 * and resizes. */
function useScrollView(ref: RefObject<HTMLElement | null>, total: number) {
  const [view, setView] = useState({ left: 0, visible: 0 });
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) return;
    const update = () => setView({ left: element.scrollLeft, visible: element.clientWidth });
    update();
    element.addEventListener("scroll", update, { passive: true });
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(update);
    observer?.observe(element);
    return () => {
      element.removeEventListener("scroll", update);
      observer?.disconnect();
    };
  }, [ref, total]);
  return view;
}

/** The temperature labels, outside the scrolling part so they stay in view. */
function TemperatureTicks({ y, unit, height }: { y: Linear; unit: string; height: number }) {
  const i18n = useI18n();
  const ticks = y.ticks(4);
  return (
    <svg className="y-axis" width={MARGIN.left} height={height} aria-hidden="true">
      {ticks.map((tick) => (
        <text key={tick} x={MARGIN.left - 6} y={y(tick)} dy="0.32em" textAnchor="end">
          {i18n.number(tick)}
          {tick === ticks.at(-1) ? ` ${unitLabel(unit)}` : ""}
        </text>
      ))}
    </svg>
  );
}

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
  y: Linear;
  extremes: Extreme[];
  right: number;
}

function TemperatureBand({ series, steps, indices, x, y, extremes, right }: BandProps) {
  const i18n = useI18n();
  const q = series.quantiles;
  const present = BANDS.filter(([lo, hi]) => q[lo] && q[hi]);
  const px = (i: number) => x(steps[i] as Date);
  const median = q.p50;

  return (
    <g className="temperature">
      {y.ticks(4).map((tick) => (
        <g key={tick} className="grid">
          <line x1={0} x2={right} y1={y(tick)} y2={y(tick)} />
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
  pictogram: number;
  rowHeight: number;
  base: string;
}

function PictogramRow({ row, steps, indices, x, end, pictogram, rowHeight, base }: RowProps) {
  const i18n = useI18n();
  const cy = row.y + rowHeight / 2;
  return (
    <g className="pictograms" data-variable={row.variable}>
      {indices.map((i) => {
        const item = row.series.items[i];
        const at = pictogramTime(steps[i] as Date, row.windowMs, end);
        if (!item || !at) return null;
        const cx = x(at);
        const label = `${i18n.classLabel(row.variable, item.class)} (${i18n.t.levels[item.level] ?? item.level})`;
        return (
          <image
            key={i}
            href={base + item.pictogram}
            x={cx - pictogram / 2}
            y={cy - pictogram / 2}
            width={pictogram}
            height={pictogram}
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
  stepHours: number;
}

function TimeAxis({ bands, steps, indices, x, top, timeZone, cell, stepHours }: AxisProps) {
  const i18n = useI18n();
  // Hourly steps: a tick every hour, a label every third.
  const every = stepHours < 3 ? 3 / stepHours : 1;
  return (
    <g className="axis">
      {indices.map((i) => {
        const t = steps[i] as Date;
        const hour = i18n.time(t, timeZone).slice(0, 2);
        // Six-hour steps are all labelled (on the day the clocks change they
        // land on 01, 07 ...); hourly ones on 00, 03, 06 ...
        const onGrid = every === 1 || Number(hour) % 3 === 0;
        const labelled =
          onGrid && (cell * every >= HOUR_LABEL_PX || (cell * every * 2 >= HOUR_LABEL_PX && hour === "12"));
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
  rowHeight: number;
}

function Crosshair({ x, at, top, bottom, windowRow, windowEnd, rowHeight }: CrosshairProps) {
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
          height={rowHeight}
        />
      )}
      <line x1={cx} x2={cx} y1={top} y2={bottom} />
    </g>
  );
}
