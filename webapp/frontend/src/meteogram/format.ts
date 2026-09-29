// Units as people read them.
const UNIT_LABELS: Record<string, string> = { degC: "°C", percent: "%", mm: "mm", "m/s": "m/s" };

export function unitLabel(unit: string): string {
  return UNIT_LABELS[unit] ?? unit;
}

/** Digits worth showing for a variable. */
export function digitsFor(variable: string): number {
  return variable === "precipitation" ? 1 : 0;
}
