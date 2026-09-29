import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError, api, errorMessage } from "./client";

describe("errorMessage", () => {
  it("takes a plain detail", () => {
    expect(errorMessage(404, { detail: "no fixture within 0.5 degrees" })).toBe("no fixture within 0.5 degrees");
  });

  it("joins FastAPI's validation errors", () => {
    const body = { detail: [{ loc: ["query", "lat"], msg: "Input should be less than or equal to 90" }] };
    expect(errorMessage(422, body)).toBe("Input should be less than or equal to 90");
  });

  it("falls back to the status", () => {
    expect(errorMessage(502, null)).toBe("HTTP 502");
    expect(errorMessage(502, { detail: [] })).toBe("HTTP 502");
  });
});

describe("api", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("builds the query and leaves out what is unset", async () => {
    const fetch = vi.fn(async (_url: string) => new Response(JSON.stringify({ steps: [] }), { status: 200 }));
    vi.stubGlobal("fetch", fetch);
    await api.forecast({ lat: 52.26, lon: 10.52, product: null, variant: "ensemble", name: "Braunschweig" });
    expect(fetch.mock.calls[0]?.[0]).toBe("/api/forecast?lat=52.26&lon=10.52&variant=ensemble&name=Braunschweig");
  });

  it("raises an ApiError carrying status and detail", async () => {
    vi.stubGlobal("fetch", async () => new Response(JSON.stringify({ detail: "upstream down" }), { status: 502 }));
    await expect(api.schemes()).rejects.toEqual(new ApiError(502, "upstream down"));
  });

  it("copes with an error page that is not JSON", async () => {
    vi.stubGlobal("fetch", async () => new Response("<html>Bad Gateway</html>", { status: 502 }));
    await expect(api.products()).rejects.toMatchObject({ status: 502, message: "HTTP 502" });
  });
});
