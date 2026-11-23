import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { I18nContext, makeI18n } from "../i18n";
import { START, forecast } from "../test/fixtures";
import { Examples } from "./Examples";

const PLACES = [
  { name: "Braunschweig", lat: 52.26, lon: 10.52 },
  { name: "Nowhere", lat: 0, lon: 0 },
];

let fetch: ReturnType<typeof vi.fn>;

function answer(places = PLACES) {
  return vi.fn(async (input: string) => {
    const url = new URL(input, "http://test");
    const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
    if (url.pathname === "/api/examples") return json({ lang: url.searchParams.get("lang"), places });
    if (url.pathname === "/api/schemes") return json({ version: "v1", pictogram_base: "/pictograms/v1/", quantiles: [], schemes: [] });
    if (url.pathname === "/api/forecast") {
      return url.searchParams.get("lat") === "0" ? json({ detail: "no data" }, 404) : json(forecast(16));
    }
    return json({ detail: "not found" }, 404);
  });
}

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ["Date"] });
  vi.setSystemTime(new Date(START.getTime() + 13 * 3_600_000));
  fetch = answer();
  vi.stubGlobal("fetch", fetch);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
});

function draw(lang: "de" | "en" = "de", onSelect = vi.fn()) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const view = render(
    <QueryClientProvider client={client}>
      <I18nContext.Provider value={makeI18n(lang)}>
        <Examples onSelect={onSelect} />
      </I18nContext.Provider>
    </QueryClientProvider>,
  );
  return { onSelect, ...view };
}

async function card(name: string) {
  const link = await screen.findByRole("link", { name });
  return link.closest("article") as HTMLElement;
}

describe("Examples", () => {
  it("asks for the places in the page's language", async () => {
    draw("de");
    await screen.findByRole("heading", { name: "Beispielorte" });
    const asked = fetch.mock.calls.map(([url]) => String(url));
    expect(asked).toContain("/api/examples?lang=de");
  });

  it("asks each place for three days in 6-hour steps, the model chosen automatically", async () => {
    draw();
    await card("Braunschweig");
    await waitFor(() => {
      const asked = fetch.mock.calls.map(([url]) => String(url)).filter((u) => u.startsWith("/api/forecast"));
      expect(asked).toContain("/api/forecast?lat=52.26&lon=10.52&days=3&step_hours=6&variant=ensemble");
    });
  });

  it("shows three days of four steps, with the rain and cloud pictograms and the temperature", async () => {
    draw("de");
    const braunschweig = await card("Braunschweig");
    await waitFor(() => expect(braunschweig.querySelectorAll(".example-day[data-day]")).toHaveLength(3));
    const today = braunschweig.querySelector('[data-day="2026-09-29"]') as HTMLElement;
    expect(within(today).getByText("Heute")).toBeInTheDocument();
    const clouds = today.querySelectorAll('[data-variable="cloud_cover"] img');
    expect(clouds).toHaveLength(4);
    expect(clouds[0]?.getAttribute("src")).toBe("/pictograms/v1/cloud/step3_sunny.svg");
    expect(clouds[0]?.getAttribute("alt")).toBe("klar (sicher)");
    expect(today.querySelectorAll('[data-variable="precipitation"] img')).toHaveLength(4);
    expect(within(today).getByLabelText("Temperatur (Median) 10 bis 13 °C")).toHaveTextContent("10–13 °C");
    // 13:00 local: the first two steps are over.
    expect(Array.from(clouds).map((c) => c.classList.contains("past"))).toEqual([true, true, false, false]);
  });

  it("says so where a place has no forecast", async () => {
    draw("en");
    const nowhere = await card("Nowhere");
    await within(nowhere).findByText("No forecast available.");
  });

  it("opens the place on a click, and leaves a ctrl-click to the browser", async () => {
    window.history.replaceState(null, "", "/?lang=de");
    const { onSelect } = draw();
    const link = await screen.findByRole("link", { name: "Braunschweig" });
    expect(link.getAttribute("href")).toBe("/?lat=52.26&lon=10.52&name=Braunschweig&lang=de");
    fireEvent.click(link, { ctrlKey: true });
    expect(onSelect).not.toHaveBeenCalled();
    fireEvent.click(link);
    expect(onSelect).toHaveBeenCalledWith({ lat: 52.26, lon: 10.52, name: "Braunschweig" });
  });

  it("is not there without places", async () => {
    fetch = answer([]);
    vi.stubGlobal("fetch", fetch);
    const { container } = draw();
    await waitFor(() => expect(fetch).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(container).toBeEmptyDOMElement();
  });
});
