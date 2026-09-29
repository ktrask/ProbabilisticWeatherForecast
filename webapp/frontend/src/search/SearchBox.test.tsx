import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { I18nContext, makeI18n } from "../i18n";
import { DEBOUNCE_MS, SearchBox, parseCoordinates } from "./SearchBox";

const PARIS = {
  query: "Paris",
  results: [
    { name: "Paris", lat: 48.85, lon: 2.35, admin1: "Île-de-France", country: "France", country_code: "FR" },
    { name: "Paris", lat: 33.66, lon: -95.56, admin1: "Texas", country: "United States", country_code: "US" },
  ],
};

let fetch: ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  fetch = vi.fn(async () => new Response(JSON.stringify(PARIS), { status: 200 }));
  vi.stubGlobal("fetch", fetch);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

function draw(onSelect = vi.fn()) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <I18nContext.Provider value={makeI18n("en")}>
        <SearchBox onSelect={onSelect} />
      </I18nContext.Provider>
    </QueryClientProvider>,
  );
  return { onSelect, input: screen.getByRole("combobox") };
}

async function type(input: HTMLElement, text: string) {
  for (let i = 1; i <= text.length; i++) {
    fireEvent.change(input, { target: { value: text.slice(0, i) } });
    await act(async () => {
      vi.advanceTimersByTime(40); // faster than the debounce
    });
  }
  await act(async () => {
    vi.advanceTimersByTime(DEBOUNCE_MS + 10);
  });
}

describe("parseCoordinates", () => {
  it.each([
    ["52.26, 10.52", { lat: 52.26, lon: 10.52 }],
    ["52.26 10.52", { lat: 52.26, lon: 10.52 }],
    ["-23.7;133.88", { lat: -23.7, lon: 133.88 }],
    ["52,26", null], // German for 52.26, not 52° N 26° E
    ["52.26,10.52", { lat: 52.26, lon: 10.52 }], // unambiguous: the decimals use dots
    ["52, 26", { lat: 52, lon: 26 }],
    ["95, 10", null],
    ["Paris", null],
  ])("%s", (text, expected) => {
    expect(parseCoordinates(text)).toEqual(expected);
  });
});

describe("SearchBox", () => {
  it("asks the server once the typing pauses, not per keystroke", async () => {
    const { input } = draw();
    await type(input, "Paris");
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0]?.[0]).toBe("/api/geocode?q=Paris&lang=en&count=6");
    expect(await screen.findAllByRole("option")).toHaveLength(2);
    expect(screen.getByText("Île-de-France, France")).toBeInTheDocument();
  });

  it("does not ask for one character", async () => {
    const { input } = draw();
    await type(input, "P");
    expect(fetch).not.toHaveBeenCalled();
    expect(screen.queryByRole("listbox")).toBeNull();
  });

  it("picks with the keyboard", async () => {
    const { input, onSelect } = draw();
    await type(input, "Paris");
    await screen.findAllByRole("option");
    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onSelect).toHaveBeenCalledWith({ lat: 33.66, lon: -95.56, name: "Paris, United States" });
    expect(input).toHaveValue("");
  });

  it("takes coordinates without asking anyone", async () => {
    const { input, onSelect } = draw();
    await type(input, "52.26, 10.52");
    expect(fetch).not.toHaveBeenCalled();
    expect(screen.getByRole("option")).toHaveTextContent("52.26, 10.52Coordinates");
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onSelect).toHaveBeenCalledWith({ lat: 52.26, lon: 10.52, name: "52.26, 10.52" });
  });

  it("says so when nothing matches", async () => {
    fetch.mockImplementation(async () => new Response(JSON.stringify({ query: "xqz", results: [] }), { status: 200 }));
    const { input } = draw();
    await type(input, "xqz");
    expect(await screen.findByText("No place found.")).toBeInTheDocument();
  });
});
