import { describe, expect, it } from "vitest";

import { STRINGS, detectLang, makeI18n } from "./i18n";

describe("i18n", () => {
  it("picks the language from ?lang=, then the browser", () => {
    expect(detectLang("?lang=en", ["de-DE"])).toBe("en");
    expect(detectLang("", ["de-AT", "en"])).toBe("de");
    expect(detectLang("", ["fr-FR", "en-US"])).toBe("en");
    expect(detectLang("?lang=fr", ["de"])).toBe("de");
  });

  it("names classes and groups of classes", () => {
    const de = makeI18n("de");
    expect(de.classLabel("precipitation", "light")).toBe("leichter Regen");
    expect(de.classLabel("precipitation", "none+light")).toBe("kein Regen oder leichter Regen");
    expect(de.classLabel("wind_speed_10m", "calm+light+strong+storm")).toBe("alles möglich");
    expect(de.classLabel("snow", "deep")).toBe("deep");
  });

  it("formats numbers per language, without a minus zero", () => {
    expect(makeI18n("de").number(0.25, 1)).toBe("0,3");
    expect(makeI18n("en").number(0.25, 1)).toBe("0.3");
    expect(makeI18n("de").number(-0.3)).toBe("0");
    expect(makeI18n("de").number(-0.6)).toBe("-1");
  });

  it("formats times in the forecast's zone", () => {
    const noonUtc = new Date("2026-09-29T12:00:00Z");
    expect(makeI18n("de").time(noonUtc, "Europe/Berlin")).toBe("14:00");
    expect(makeI18n("de").time(noonUtc, "Australia/Darwin")).toBe("21:30");
    expect(makeI18n("en").weekday(noonUtc, "Pacific/Kiritimati", "long")).toBe("Wednesday");
  });

  it("has the same keys in both languages", () => {
    expect(Object.keys(STRINGS.en).sort()).toEqual(Object.keys(STRINGS.de).sort());
    for (const variable of Object.keys(STRINGS.de.classes)) {
      expect(Object.keys(STRINGS.en.classes[variable] ?? {}).sort()).toEqual(
        Object.keys(STRINGS.de.classes[variable] ?? {}).sort(),
      );
    }
  });
});
