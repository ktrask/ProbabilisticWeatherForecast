// The legend, drawn from /api/schemes rather than kept as an image: it shows
// exactly the pictograms and thresholds the backend is configured with.
import type { Scheme } from "../api/client";
import { type I18n, useI18n } from "../i18n";
import { unitLabel } from "../meteogram/format";

export interface LegendProps {
  schemes: Scheme[];
  pictogramBase: string;
}

export function Legend({ schemes, pictogramBase }: LegendProps) {
  const i18n = useI18n();
  return (
    <section className="legend" aria-labelledby="legend-title">
      <h2 id="legend-title">{i18n.t.legend}</h2>
      <p className="hint">{i18n.t.legendIntro}</p>
      <div className="legend-schemes">
        {schemes.map((scheme) => (
          <LegendScheme key={scheme.name} scheme={scheme} pictogramBase={pictogramBase} />
        ))}
      </div>
    </section>
  );
}

function LegendScheme({ scheme, pictogramBase }: { scheme: Scheme; pictogramBase: string }) {
  const i18n = useI18n();
  const levels = [...new Set(scheme.outcomes.map((o) => o.level))].sort((a, b) => b - a);
  // Rules schemes may reach one pictogram by several rules; show it once.
  const seen = new Set<string>();
  return (
    <figure className="legend-scheme" data-scheme={scheme.name}>
      <figcaption>
        {i18n.t.variables[scheme.variable] ?? scheme.variable}
        {scheme.window_hours ? <span className="unit"> ({unitLabel(scheme.unit)} / {scheme.window_hours} h)</span> : null}
      </figcaption>
      {levels.map((level) => (
        <div key={level} className="legend-level" data-level={level}>
          <div className="level-name">
            {i18n.t.levels[level] ?? level}
            <small className="level-rule">{levelRule(scheme, level, i18n)}</small>
          </div>
          <ul>
            {scheme.outcomes
              .filter((o) => o.level === level)
              .filter((o) => {
                const key = `${o.pictogram}|${o.class}`;
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
              })
              .map((o) => (
                <li key={`${o.pictogram}|${o.class}`}>
                  <img src={pictogramBase + o.pictogram} alt="" width={40} height={40} />
                  <span>
                    {i18n.classLabel(scheme.variable, o.class)}
                    <small className="range">{range(scheme, o.class, i18n)}</small>
                  </span>
                </li>
              ))}
          </ul>
        </div>
      ))}
    </figure>
  );
}

/** What a tree scheme's level asks of the ensemble: "at least 66 % of members
 * in one class" for an interval p17..p83, "otherwise" for the level that
 * always applies. Nothing for rules schemes, whose levels are hand-written. */
export function levelRule(scheme: Scheme, level: number, i18n: I18n): string {
  const spec = scheme.levels?.find((l) => l.level === level);
  if (!spec) return "";
  if (!spec.interval) return i18n.t.otherwise;
  const [low, high] = spec.interval.map((q) => Number(q.slice(1)));
  if (low === undefined || high === undefined) return "";
  const grouped = scheme.outcomes.some((o) => o.level === level && o.class.includes("+"));
  return i18n.t.levelRule(high - low, grouped);
}

/** "0.1–1 mm" for a tree scheme's class or group; nothing for rules schemes. */
export function range(scheme: Scheme, cls: string, i18n: I18n): string {
  const classes = scheme.classes;
  if (!classes) return "";
  const ids = classes.map((c) => c.id);
  const parts = cls.split("+");
  const first = ids.indexOf(parts[0] ?? "");
  const last = ids.indexOf(parts[parts.length - 1] ?? "");
  if (first < 0 || last < 0) return "";
  const lower = first > 0 ? classes[first - 1]?.below ?? null : null;
  const upper = classes[last]?.below ?? null;
  const unit = unitLabel(scheme.unit);
  const n = (v: number) => i18n.number(v, Number.isInteger(v) ? 0 : 1);
  if (lower === null && upper === null) return "";
  if (lower === null) return `< ${n(upper as number)} ${unit}`;
  if (upper === null) return `≥ ${n(lower)} ${unit}`;
  return `${n(lower)}–${n(upper)} ${unit}`;
}
