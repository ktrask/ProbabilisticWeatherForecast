import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { I18nContext, makeI18n } from "../i18n";
import { forecast } from "../test/fixtures";
import { Meteogram } from "./Meteogram";

function draw(from = 0, to = 12, lang: "de" | "en" = "en", width = 900, steps = 12) {
  return render(
    <I18nContext.Provider value={makeI18n(lang)}>
      <Meteogram forecast={forecast(steps)} pictogramBase="/pictograms/v1/" window={{ from, to, stale: false }} width={width} />
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

  describe("on a narrow screen", () => {
    // 360 px is a phone: sections hold five days (21 steps). Eight days from
    // local midnight - 32 steps - are cut at the midnight nearest the middle,
    // step 16, into two sections sharing that step.
    const narrow = () => draw(0, 32, "en", 360, 32);
    const sectionsOf = (container: HTMLElement) =>
      Array.from(container.querySelectorAll(".section")).map((el) => [
        Number(el.getAttribute("data-first")),
        Number(el.getAttribute("data-last")),
      ]);

    it("splits into sections, one below the other", () => {
      const { container } = narrow();
      expect(sectionsOf(container)).toEqual([
        [0, 16],
        [16, 31],
      ]);
    });

    it("keeps five days in one row, even on a phone", () => {
      const { container } = draw(0, 21, "en", 360, 32);
      expect(container.querySelectorAll(".section")).toHaveLength(1);
    });

    it("draws every total exactly once across the cut", () => {
      const { container } = narrow();
      const rain = images(container, "precipitation").map((i) => i.querySelector("title")?.textContent);
      expect(rain).toHaveLength(31); // as in one row: the last step's window is beyond the end
      // The shared step's instants appear at the end of one section and the start of the next.
      expect(images(container, "cloud_cover")).toHaveLength(33);
    });

    it("uses one time scale and one temperature scale for all sections", () => {
      const { container } = narrow();
      const svgs = Array.from(container.querySelectorAll("svg"));
      const perStep = svgs.map((svg) => {
        const xs = Array.from(svg.querySelectorAll('g[data-variable="cloud_cover"] image')).map((i) =>
          Number(i.getAttribute("x")),
        );
        return (xs[1] as number) - (xs[0] as number);
      });
      expect(perStep[0]).toBeCloseTo(perStep[1] as number, 6);
      const ticks = svgs.map((svg) => Array.from(svg.querySelectorAll(".grid text")).map((t) => t.textContent));
      expect(ticks[0]).toEqual(ticks[1]);
    });

    it("walks through the cut with the keyboard", () => {
      const { container } = narrow();
      const chart = screen.getByRole("group");
      fireEvent.keyDown(chart, { key: "Home" });
      for (let i = 0; i < 16; i++) fireEvent.keyDown(chart, { key: "ArrowRight" });
      // Step 16, shared by both sections, is shown in the later one.
      const second = container.querySelectorAll(".section")[1] as HTMLElement;
      expect(second.querySelector(".crosshair")).not.toBeNull();
      expect(container.querySelectorAll(".crosshair")).toHaveLength(1);
      expect(screen.getByTestId("tooltip")).toHaveTextContent("Saturday 03/10, 00:00");
    });
  });
});
