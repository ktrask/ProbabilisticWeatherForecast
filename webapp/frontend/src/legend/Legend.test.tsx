import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { I18nContext, makeI18n } from "../i18n";
import { rainScheme, windRules } from "../test/fixtures";
import { Legend, range } from "./Legend";

function draw(lang: "de" | "en" = "en") {
  return render(
    <I18nContext.Provider value={makeI18n(lang)}>
      <Legend schemes={[rainScheme, windRules]} pictogramBase="/pictograms/v1/" />
    </I18nContext.Provider>,
  );
}

describe("Legend", () => {
  it("groups each scheme's pictograms by certainty, most certain first", () => {
    const { container } = draw();
    const rain = container.querySelector('[data-scheme="precipitation-vsup"]') as HTMLElement;
    const levels = Array.from(rain.querySelectorAll(".legend-level")).map((l) => l.getAttribute("data-level"));
    expect(levels).toEqual(["3", "2", "1"]);
    const certain = rain.querySelector('[data-level="3"]') as HTMLElement;
    expect(within(certain).getAllByRole("listitem")).toHaveLength(4);
    expect(within(certain).getByText("certain")).toBeInTheDocument();
  });

  it("uses the scheme's own pictograms", () => {
    const { container } = draw();
    const srcs = Array.from(container.querySelectorAll('[data-scheme="precipitation-vsup"] img')).map((i) =>
      i.getAttribute("src"),
    );
    expect(srcs[0]).toBe("/pictograms/v1/rain/dry.svg");
    expect(srcs).toHaveLength(7);
  });

  it("shows a rules scheme's pictogram once even if several rules pick it", () => {
    const { container } = draw();
    expect(container.querySelectorAll('[data-scheme="wind-legacy"] img')).toHaveLength(3);
  });

  it("states the window of totals", () => {
    draw();
    expect(screen.getByText("(mm / 6 h)")).toBeInTheDocument();
  });

  it("gives the thresholds of tree classes and groups", () => {
    const de = makeI18n("de");
    expect(range(rainScheme, "none", de)).toBe("< 0,1 mm");
    expect(range(rainScheme, "light", de)).toBe("0,1–1 mm");
    expect(range(rainScheme, "heavy", de)).toBe("≥ 2 mm");
    expect(range(rainScheme, "none+light", de)).toBe("< 1 mm");
    expect(range(rainScheme, "medium+heavy", de)).toBe("≥ 1 mm");
    expect(range(rainScheme, "none+light+medium+heavy", de)).toBe("");
    expect(range(windRules, "calm", de)).toBe("");
  });
});
