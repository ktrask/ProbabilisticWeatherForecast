// German and English. Dates, times and numbers go through Intl, in the
// forecast location's own time zone - the API sends UTC and leaves local time
// to the client.
import { createContext, useContext } from "react";

export type Lang = "de" | "en";

export function detectLang(search: string, languages: readonly string[]): Lang {
  const forced = new URLSearchParams(search).get("lang");
  if (forced === "de" || forced === "en") return forced;
  return languages.some((l) => l.toLowerCase().startsWith("de")) ? "de" : "en";
}

const de = {
  title: "Probabilistisches Meteogramm",
  tagline: "Je unsicherer die Vorhersage, desto vager das Symbol.",
  searchLabel: "Ort suchen",
  searchPlaceholder: "Ort oder Koordinaten, z. B. Braunschweig",
  coordinates: "Koordinaten",
  myLocation: "Mein Standort",
  locating: "Standort wird ermittelt …",
  locationDenied: "Der Standort ist nicht verfügbar.",
  noResults: "Kein Ort gefunden.",
  searching: "Suche …",
  intro: "Suche einen Ort, um seine Vorhersage zu sehen.",
  loading: "Vorhersage wird geladen …",
  retry: "Erneut versuchen",
  errorTitle: "Die Vorhersage konnte nicht geladen werden.",
  noData: "Für diesen Ort gibt es keine Vorhersage.",
  modelNoData: "Das gewählte Modell hat für diesen Ort keine Vorhersage.",
  useDefault: "Automatisch wählen",
  days: "Tage",
  product: "Modell",
  automatic: (label: string) => `Automatisch (${label})`,
  chosenAutomatically: (label: string) => `${label}, automatisch gewählt`,
  productDetails: (members: number, gridKm: string, days: string) =>
    `${members} Mitglieder, ${gridKm} km, bis ${days} Tage`,
  layout: "Darstellung",
  stepLabel: "Schritte",
  stepName: (hours: number) => (hours === 1 ? "stündlich" : `${hours} h`),
  horizontal: "Waagerecht",
  vertical: "Senkrecht",
  earlier: "Früher",
  later: "Später",
  stale: "Diese Vorhersage ist veraltet: Sie reicht nicht bis heute. Angezeigt wird sie ab ihrem ersten Zeitschritt.",
  legend: "Legende",
  legendIntro:
    "Ein Symbol zeigt, was das Ensemble für den Zeitschritt erwartet. Streuen die Mitglieder über mehrere Klassen, wird es unschärfer.",
  source: (label: string, members: number | null) =>
    `${label}${members ? `, ${members} Ensemble-Mitglieder` : ""}. Daten: Open-Meteo.`,
  variables: {
    temperature_2m: "Temperatur",
    precipitation: "Niederschlag",
    cloud_cover: "Bewölkung",
    wind_speed_10m: "Wind",
  } as Record<string, string>,
  // Column heads of the vertical layout, where there is little room.
  shortNames: {
    temperature_2m: "Temperatur",
    precipitation: "Regen",
    cloud_cover: "Wolken",
    wind_speed_10m: "Wind",
  } as Record<string, string>,
  levels: { 3: "sicher", 2: "wahrscheinlich", 1: "unsicher" } as Record<number, string>,
  levelRule: (share: number, grouped: boolean) =>
    `mind. ${share} % der Mitglieder in ${grouped ? "einer Gruppe" : "einer Klasse"}`,
  otherwise: "sonst",
  classes: {
    precipitation: { none: "kein Regen", light: "leichter Regen", medium: "mäßiger Regen", heavy: "starker Regen" },
    cloud_cover: { clear: "klar", light: "leicht bewölkt", cloudy: "bewölkt", overcast: "bedeckt" },
    wind_speed_10m: { calm: "windstill", light: "leichter Wind", strong: "starker Wind", storm: "Sturm" },
  } as Record<string, Record<string, string>>,
  or: "oder",
  anything: "alles möglich",
  range: "Spanne",
  median: "Median",
  upTo: "bis",
  chartLabel: (place: string) => `Meteogramm für ${place}. Mit den Pfeiltasten durch die Zeitschritte.`,
};

type Strings = typeof de;

const en: Strings = {
  title: "Probabilistic meteogram",
  tagline: "The less certain the forecast, the vaguer the symbol.",
  searchLabel: "Search a place",
  searchPlaceholder: "Place or coordinates, e.g. Braunschweig",
  coordinates: "Coordinates",
  myLocation: "My location",
  locating: "Finding your location …",
  locationDenied: "Your location is not available.",
  noResults: "No place found.",
  searching: "Searching …",
  intro: "Search a place to see its forecast.",
  loading: "Loading the forecast …",
  retry: "Try again",
  errorTitle: "The forecast could not be loaded.",
  noData: "There is no forecast for this place.",
  modelNoData: "The chosen model has no forecast for this place.",
  useDefault: "Choose automatically",
  days: "Days",
  product: "Model",
  automatic: (label) => `Automatic (${label})`,
  chosenAutomatically: (label) => `${label}, chosen automatically`,
  productDetails: (members, gridKm, days) => `${members} members, ${gridKm} km, up to ${days} days`,
  layout: "Layout",
  stepLabel: "Steps",
  stepName: (hours) => (hours === 1 ? "hourly" : `${hours} h`),
  horizontal: "Horizontal",
  vertical: "Vertical",
  earlier: "Earlier",
  later: "Later",
  stale: "This forecast is out of date: it does not reach today. It is shown from its first step.",
  legend: "Legend",
  legendIntro:
    "A symbol shows what the ensemble expects for the step. The more its members spread across classes, the vaguer it gets.",
  source: (label, members) => `${label}${members ? `, ${members} ensemble members` : ""}. Data: Open-Meteo.`,
  variables: {
    temperature_2m: "Temperature",
    precipitation: "Precipitation",
    cloud_cover: "Cloud cover",
    wind_speed_10m: "Wind",
  },
  shortNames: {
    temperature_2m: "Temperature",
    precipitation: "Rain",
    cloud_cover: "Clouds",
    wind_speed_10m: "Wind",
  },
  levels: { 3: "certain", 2: "likely", 1: "uncertain" },
  levelRule: (share, grouped) => `at least ${share} % of members in ${grouped ? "one group" : "one class"}`,
  otherwise: "otherwise",
  classes: {
    precipitation: { none: "no rain", light: "light rain", medium: "moderate rain", heavy: "heavy rain" },
    cloud_cover: { clear: "clear", light: "partly cloudy", cloudy: "cloudy", overcast: "overcast" },
    wind_speed_10m: { calm: "calm", light: "light wind", strong: "strong wind", storm: "storm" },
  },
  or: "or",
  anything: "anything possible",
  range: "range",
  median: "median",
  upTo: "up to",
  chartLabel: (place) => `Meteogram for ${place}. Use the arrow keys to step through time.`,
};

export const STRINGS: Record<Lang, Strings> = { de, en };

export interface I18n {
  lang: Lang;
  t: Strings;
  /** "none+light" of precipitation -> "kein Regen oder leichter Regen". */
  classLabel(variable: string, cls: string): string;
  number(value: number, digits?: number): string;
  time(date: Date, timeZone: string): string;
  weekday(date: Date, timeZone: string, style?: "short" | "long"): string;
  dayMonth(date: Date, timeZone: string): string;
}

export function makeI18n(lang: Lang): I18n {
  const t = STRINGS[lang];
  const locale = lang === "de" ? "de-DE" : "en-GB";
  const numberFormats = new Map<number, Intl.NumberFormat>();
  const dateFormats = new Map<string, Intl.DateTimeFormat>();
  const dateFormat = (timeZone: string, options: Intl.DateTimeFormatOptions) => {
    const key = `${timeZone}|${JSON.stringify(options)}`;
    let format = dateFormats.get(key);
    if (!format) {
      format = new Intl.DateTimeFormat(locale, { timeZone, ...options });
      dateFormats.set(key, format);
    }
    return format;
  };
  return {
    lang,
    t,
    classLabel(variable, cls) {
      const parts = cls.split("+");
      const names = t.classes[variable] ?? {};
      if (parts.length === Object.keys(names).length && parts.length > 2) return t.anything;
      return parts.map((part) => names[part] ?? part).join(` ${t.or} `);
    },
    number(value, digits = 0) {
      let format = numberFormats.get(digits);
      if (!format) {
        format = new Intl.NumberFormat(locale, { minimumFractionDigits: digits, maximumFractionDigits: digits });
        numberFormats.set(digits, format);
      }
      // No "-0": a rounded -0.3 °C reads as 0, not minus zero.
      return format.format(Math.abs(value) < 0.5 * 10 ** -digits ? 0 : value);
    },
    time: (date, timeZone) => dateFormat(timeZone, { hour: "2-digit", minute: "2-digit" }).format(date),
    weekday: (date, timeZone, style = "short") => dateFormat(timeZone, { weekday: style }).format(date),
    dayMonth: (date, timeZone) => dateFormat(timeZone, { day: "numeric", month: "numeric" }).format(date),
  };
}

export const I18nContext = createContext<I18n>(makeI18n("en"));

export function useI18n(): I18n {
  return useContext(I18nContext);
}
