// Below a row that scrolls: a map of the days in the whole forecast with a
// frame over the part in view. Dragging along it or tapping it scrolls the row
// there; the arrows page through it, for mice without a sideways wheel.
import { type CSSProperties, type PointerEvent, type RefObject, useRef } from "react";

import { useI18n } from "../i18n";
import type { DayBand } from "./layout";

type Scale = (t: Date) => number;
// A day shorter than this on the map - the evening before the first step - is
// shaded but not named.
const NAMED_DAY_MS = 12 * 3_600_000;

export interface ScrollIndicatorProps {
  scroller: RefObject<HTMLDivElement | null>;
  left: number; // scrolled this far
  visible: number; // pixels in view
  total: number; // pixels of the whole row
  bands: DayBand[];
  x: Scale;
  timeZone: string;
  style?: CSSProperties;
}

export function ScrollIndicator({ scroller, left, visible, total, bands, x, timeZone, style }: ScrollIndicatorProps) {
  const i18n = useI18n();
  const track = useRef<HTMLDivElement>(null);
  const share = (px: number) => `${(100 * px) / Math.max(1, total)}%`;
  const atStart = left <= 1;
  const atEnd = left + visible >= total - 1;

  // Centre the view on the point of the map under the pointer.
  const seek = (event: PointerEvent<HTMLDivElement>) => {
    const element = scroller.current;
    const box = track.current?.getBoundingClientRect();
    if (!element || !box || box.width === 0) return;
    const at = ((event.clientX - box.left) / box.width) * total;
    element.scrollLeft = at - element.clientWidth / 2;
  };
  const page = (direction: number) => {
    const element = scroller.current;
    if (!element) return;
    element.scrollBy({ left: direction * element.clientWidth * 0.8, behavior: "smooth" });
  };

  return (
    <div className="scroll-indicator" data-testid="scroll-indicator" style={style}>
      <button type="button" className="page" aria-label={i18n.t.earlier} disabled={atStart} onClick={() => page(-1)}>
        ‹
      </button>
      <div
        className="track"
        ref={track}
        aria-hidden="true"
        onPointerDown={(event) => {
          event.currentTarget.setPointerCapture(event.pointerId);
          seek(event);
        }}
        onPointerMove={(event) => {
          if (event.currentTarget.hasPointerCapture(event.pointerId)) seek(event);
        }}
      >
        {bands.map((band) => {
          const from = x(band.start);
          const span = x(band.end) - from;
          const middle = new Date((band.start.getTime() + band.end.getTime()) / 2);
          return (
            <span
              key={band.key}
              className={band.shaded ? "map-day shaded" : "map-day"}
              style={{ left: share(from), width: share(span) }}
            >
              {band.end.getTime() - band.start.getTime() >= NAMED_DAY_MS
                ? i18n.weekday(middle, timeZone, "short").replace(/\.$/, "")
                : ""}
            </span>
          );
        })}
        <div className="thumb" data-testid="scroll-thumb" style={{ left: share(left), width: share(visible) }} />
      </div>
      <button type="button" className="page" aria-label={i18n.t.later} disabled={atEnd} onClick={() => page(1)}>
        ›
      </button>
    </div>
  );
}
