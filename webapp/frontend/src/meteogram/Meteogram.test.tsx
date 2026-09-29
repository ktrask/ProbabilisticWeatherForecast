import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { I18nContext, makeI18n } from "../i18n";
import { forecast } from "../test/fixtures";
import { Meteogram } from "./Meteogram";

function draw(from = 0, to = 12, lang: "de" | "en" = "en") {
  return render(
    <I18nContext.Provider value={makeI18n(lang)}>
      <Meteogram forecast={forecast()} pictogramBase="/pictograms/v1/" window={{ from, to, stale: false }} width={900} />
    </I18nContext.Provider>,
  );
}

const images = (container: HTMLElement, variable: string) =>
  Array.from(container.querySelectorAll(`g[data-variable="${variable}"] image`));

describe("Meteogram", () => {
  it("draws a pictogram per step for instants", () => {
    const { container } = draw();
    expect(images(container, "cloud_cover")).toHaveLength(12);
    expect(images(container, "wind_speed_10m")).toHaveLength(12);
  });

  it("draws a total only when its whole window is on the chart", () => {
    // The last step's window reaches beyond the chart's right edge.
    const { container } = draw();
    expect(images(container, "precipitation")).toHaveLength(11);
  });

  it("places a total between the two instants that bound its window", () => {
    const { container } = draw();
    const cloud = images(container, "cloud_cover").map((i) => Number(i.getAttribute("x")));
    const rain = images(container, "precipitation").map((i) => Number(i.getAttribute("x")));
    const half = ((cloud[1] as number) - (cloud[0] as number)) / 2;
    expect(rain[0]).toBeCloseTo((cloud[0] as number) + half, 5);
  });

  it("shows only the visible window", () => {
    const { container } = draw(4, 8);
    expect(images(container, "cloud_cover")).toHaveLength(4);
  });

  it("points pictograms at the versioned base and labels them", () => {
    const { container } = draw();
    const first = images(container, "precipitation")[0];
    expect(first?.getAttribute("href")).toBe("/pictograms/v1/rain/step2_mostly_dry.svg");
    expect(first?.querySelector("title")?.textContent).toBe("no rain or light rain (likely)");
    expect(images(container, "wind_speed_10m")[0]?.querySelector("title")?.textContent).toBe(
      "anything possible (uncertain)",
    );
  });

  it("labels the daily extremes of the median", () => {
    const { container } = draw();
    const labels = Array.from(container.querySelectorAll(".extreme text")).map((t) => t.textContent);
    // The median rises by one per step, so each local day runs from its first
    // step (min) to its last (max): 10-13, 14-17, 18-21.
    expect(labels).toEqual(["13", "10", "17", "14", "21", "18"]);
  });

  it("walks through the steps with the arrow keys", () => {
    draw();
    const chart = screen.getByRole("group");
    fireEvent.keyDown(chart, { key: "ArrowRight" });
    const tooltip = screen.getByTestId("tooltip");
    expect(tooltip).toHaveTextContent("Tuesday 29/09, 00:00");
    fireEvent.keyDown(chart, { key: "ArrowRight" });
    expect(screen.getByTestId("tooltip")).toHaveTextContent("06:00");
    expect(screen.getByTestId("tooltip")).toHaveTextContent("Precipitation 06:00–12:00");
    fireEvent.keyDown(chart, { key: "End" });
    expect(screen.getByTestId("tooltip")).toHaveTextContent("Thursday 01/10, 18:00");
    fireEvent.keyDown(chart, { key: "Escape" });
    expect(screen.queryByTestId("tooltip")).toBeNull();
  });

  it("lists every variable in the tooltip, with the step's class and certainty", () => {
    draw();
    fireEvent.keyDown(screen.getByRole("group"), { key: "Home" });
    const rows = Array.from(screen.getByTestId("tooltip").querySelectorAll(".row")).map((r) => r.textContent);
    expect(rows[0]).toBe("Temperature10 °C (7–13)");
    expect(rows[1]).toBe("Cloud cover50 % (37–63)clear · certain");
    expect(rows[3]).toContain("4 m/s (3–5)");
  });

  it("speaks German", () => {
    draw(0, 12, "de");
    fireEvent.keyDown(screen.getByRole("group"), { key: "Home" });
    expect(screen.getByTestId("tooltip")).toHaveTextContent("Dienstag 29.9., 00:00");
    expect(screen.getByRole("group")).toHaveAccessibleName(/Meteogramm für Braunschweig/);
  });
});
