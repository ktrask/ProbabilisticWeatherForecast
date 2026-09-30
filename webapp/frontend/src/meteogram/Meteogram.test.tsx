import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { I18nContext, makeI18n } from "../i18n";
import { forecast } from "../test/fixtures";
import { Meteogram, type Orientation } from "./Meteogram";
import { MIN_CELL } from "./RowChart";

function draw(from = 0, to = 12, lang: "de" | "en" = "en", width = 900, steps = 12, orientation: Orientation = "row") {
  return render(
    <I18nContext.Provider value={makeI18n(lang)}>
      <Meteogram
        forecast={forecast(steps)}
        pictogramBase="/pictograms/v1/"
        window={{ from, to, stale: false }}
        width={width}
        orientation={orientation}
      />
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

  describe("in a row too long for the screen", () => {
    // 360 px is a phone: eight days - 32 steps - need more than 300 px.
    const narrow = () => draw(0, 32, "en", 360, 32);

    it("stays in one row and scrolls sideways, instead of breaking into pieces", () => {
      const { container } = narrow();
      expect(container.querySelectorAll("svg:not(.y-axis)")).toHaveLength(1);
      expect(images(container, "cloud_cover")).toHaveLength(32);
      expect(images(container, "precipitation")).toHaveLength(31);
      const content = container.querySelector(".scroll-content") as HTMLElement;
      expect(content.style.width).toBe(`${32 * MIN_CELL}px`);
      expect(screen.getByTestId("scroller")).toHaveClass("scrolls");
    });

    it("keeps the temperature labels out of the scrolling part", () => {
      const { container } = narrow();
      const labels = Array.from(container.querySelectorAll(".y-axis text")).map((t) => t.textContent);
      expect(labels.length).toBeGreaterThan(1);
      expect(screen.getByTestId("scroller").querySelector(".grid text")).toBeNull();
    });

    it("maps the days under it, with a frame for the part in view", () => {
      narrow();
      const indicator = screen.getByTestId("scroll-indicator");
      // The first "day" is the three hours before the first step, at midnight.
      expect(Array.from(indicator.querySelectorAll(".map-day")).map((d) => d.textContent)).toEqual([
        "", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", "Mon", "Tue",
      ]);
      expect(screen.getByTestId("scroll-thumb")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Earlier" })).toBeDisabled();
    });

    it("needs no indicator when everything fits", () => {
      draw();
      expect(screen.queryByTestId("scroll-indicator")).toBeNull();
      expect(screen.getByTestId("scroller")).not.toHaveClass("scrolls");
    });
  });

  describe("in a column", () => {
    const column = () => draw(0, 12, "en", 380, 12, "column");
    const ys = (container: HTMLElement, variable: string) =>
      images(container, variable).map((i) => Number(i.getAttribute("y")));

    it("runs time down the page, one pictogram per step", () => {
      const { container } = column();
      expect(container.querySelector('[data-orientation="column"]')).not.toBeNull();
      const cloud = ys(container, "cloud_cover");
      expect(cloud).toHaveLength(12);
      expect(cloud.every((y, i) => i === 0 || y > (cloud[i - 1] as number))).toBe(true);
      expect(images(container, "precipitation")).toHaveLength(11);
    });

    it("puts a total between the instants that bound its window", () => {
      const { container } = column();
      const cloud = ys(container, "cloud_cover");
      const rain = ys(container, "precipitation");
      expect(rain[0]).toBeCloseTo(((cloud[0] as number) + (cloud[1] as number)) / 2, 5);
    });

    it("orders the columns as the row stacks them, under named heads", () => {
      const { container } = column();
      const x = (variable: string) => Number(images(container, variable)[0]?.getAttribute("x"));
      expect(x("cloud_cover")).toBeLessThan(x("precipitation"));
      expect(x("precipitation")).toBeLessThan(x("wind_speed_10m"));
      const heads = Array.from(container.querySelectorAll(".column-name")).map((t) => t.textContent);
      expect(heads).toEqual(["Clouds", "Rain", "Wind", "Temperature"]);
    });

    it("walks down the steps with the arrow keys", () => {
      column();
      const chart = screen.getByRole("group");
      fireEvent.keyDown(chart, { key: "ArrowDown" });
      expect(screen.getByTestId("tooltip")).toHaveTextContent("Tuesday 29/09, 00:00");
      fireEvent.keyDown(chart, { key: "ArrowDown" });
      expect(screen.getByTestId("tooltip")).toHaveTextContent("06:00");
      fireEvent.keyDown(chart, { key: "ArrowUp" });
      expect(screen.getByTestId("tooltip")).toHaveTextContent("00:00");
    });
  });
});
