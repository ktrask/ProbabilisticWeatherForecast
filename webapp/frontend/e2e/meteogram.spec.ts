import { readFileSync } from "node:fs";

import { type Page, expect, test } from "@playwright/test";

const FIXTURES = new URL("../../tests/fixtures/", import.meta.url);
const H = 3_600_000;

interface Location {
  name: string;
  latitude: number;
  longitude: number;
}
const LOCATIONS = JSON.parse(readFileSync(new URL("locations.json", FIXTURES), "utf8")) as Record<string, Location>;

/** The first step of a recorded forecast, in either fixture format: a Forecast
 * (its first ISO step) or a legacy allMeteogramData dict ("YYYYMMDD"/"HHMM", UTC). */
function recordedAt(key: string): Date {
  const data = JSON.parse(readFileSync(new URL(`${key}.json`, FIXTURES), "utf8"));
  if (Array.isArray(data.steps)) return new Date(data.steps[0] as string);
  const { date, time } = data["2t"] as { date: string; time: string };
  return new Date(
    Date.UTC(+date.slice(0, 4), +date.slice(4, 6) - 1, +date.slice(6, 8), +time.slice(0, 2), +time.slice(2, 4)),
  );
}

function url(key: string, extra = "") {
  const l = LOCATIONS[key] as Location;
  return `/?lat=${l.latitude}&lon=${l.longitude}&name=${encodeURIComponent(l.name)}&lang=de${extra}`;
}

/** Freeze "now" half a day into the recording, so the view starts there and
 * the picture does not drift as the recording ages. */
async function freezeClock(page: Page, key = "braunschweig") {
  await page.clock.setFixedTime(new Date(recordedAt(key).getTime() + 12 * H));
}

async function showMeteogram(page: Page, path: string) {
  const broken: string[] = [];
  page.on("response", (response) => {
    if (response.url().includes("/pictograms/") && response.status() !== 200) broken.push(response.url());
  });
  await page.goto(path);
  const chart = page.getByTestId("meteogram");
  await expect(chart.locator("image").first()).toBeAttached();
  await page.waitForLoadState("networkidle");
  expect(broken, "pictograms that did not load").toEqual([]);
  return chart;
}

test.describe("meteogram of every recorded location", () => {
  for (const key of Object.keys(LOCATIONS)) {
    test(key, async ({ page }) => {
      await freezeClock(page, key);
      const chart = await showMeteogram(page, url(key, "&days=7"));
      await expect(page.getByTestId("place")).toHaveText(LOCATIONS[key]?.name ?? "");
      await expect(chart).toHaveScreenshot(`${key}.png`);
    });
  }
});

test("on a phone @phone", async ({ page }) => {
  await freezeClock(page, "reykjavik");
  await showMeteogram(page, url("reykjavik", "&days=3"));
  await expect(page).toHaveScreenshot("reykjavik-phone.png", { fullPage: true });
});

test("a week on a phone stays in one row and scrolls sideways @phone", async ({ page }) => {
  await freezeClock(page, "braunschweig");
  const chart = await showMeteogram(page, url("braunschweig", "&days=7"));
  // Each 6-hour total once: 28 steps, the last one's window beyond the end.
  await expect(chart.locator('g[data-variable="precipitation"] image')).toHaveCount(27);
  const scroller = page.getByTestId("scroller");
  const overflow = await scroller.evaluate((el) => el.scrollWidth - el.clientWidth);
  expect(overflow).toBeGreaterThan(100);
  await expect(chart).toHaveScreenshot("braunschweig-week-phone.png");

  // A swipe to the left moves the view later, and the frame on the day map with it.
  const thumb = page.getByTestId("scroll-thumb");
  const before = (await thumb.boundingBox())?.x ?? 0;
  const box = (await scroller.boundingBox()) as { x: number; y: number; width: number; height: number };
  await page.touchscreen.tap(box.x + 5, box.y + box.height - 20); // focus without a crosshair in the way
  await scroller.evaluate((el) => el.scrollBy({ left: 300 }));
  await expect.poll(async () => (await thumb.boundingBox())?.x ?? 0).toBeGreaterThan(before + 20);

  // The arrow next to the map pages back.
  await page.getByRole("button", { name: "Früher" }).click();
  await expect.poll(() => scroller.evaluate((el) => el.scrollLeft)).toBeLessThan(300);
});

test("the vertical layout runs time down the page @phone", async ({ page }) => {
  await freezeClock(page, "braunschweig");
  await showMeteogram(page, url("braunschweig", "&days=3"));
  await page.getByRole("button", { name: "Senkrecht" }).click();
  await expect(page).toHaveURL(/layout=vertical/);
  const chart = page.getByTestId("meteogram");
  await expect(chart).toHaveAttribute("data-orientation", "column");
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(12);
  await page.waitForLoadState("networkidle");
  await expect(chart).toHaveScreenshot("braunschweig-vertical-phone.png");
  // Back to the row, and the choice leaves the URL again.
  await page.getByRole("button", { name: "Waagerecht" }).click();
  await expect(chart).toHaveAttribute("data-orientation", "row");
  await expect(page).not.toHaveURL(/layout=/);
});

test("starts at the step nearest to now, in the place's own time", async ({ page }) => {
  await freezeClock(page);
  const chart = await showMeteogram(page, url("braunschweig", "&product=ecmwf"));
  // Recorded from local midnight; twelve hours later is noon in Braunschweig,
  // whatever the browser's own zone (New York here).
  await expect(chart.locator(".axis .hour").first()).toHaveText("12");
});

test("hovering lists the step's values", async ({ page }) => {
  await freezeClock(page);
  const chart = await showMeteogram(page, url("braunschweig"));
  const box = (await chart.getByTestId("scroller").locator("svg").boundingBox()) as { x: number; y: number; width: number };
  await page.mouse.move(box.x + box.width / 2, box.y + 200);
  const tooltip = page.getByTestId("tooltip");
  await expect(tooltip).toBeVisible();
  for (const name of ["Temperatur", "Bewölkung", "Niederschlag", "Wind"]) {
    await expect(tooltip).toContainText(name);
  }
  await page.mouse.move(box.x + box.width / 2, box.y - 50);
  await expect(tooltip).toBeHidden();
});

test("changing the days fetches again only when the model changes", async ({ page }) => {
  await freezeClock(page);
  // Three days in Braunschweig: chosen automatically, ICON-EU.
  const chart = await showMeteogram(page, url("braunschweig", "&days=3"));
  let requests = 0;
  page.on("request", (request) => {
    if (request.url().includes("/api/forecast")) requests++;
  });
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(12);
  await page.getByRole("slider").fill("4");
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(16);
  await expect(page).toHaveURL(/days=4/);
  expect(requests).toBe(0); // still ICON-EU
  // Six days are more than ICON-EU reaches: ECMWF takes over.
  await page.getByRole("slider").fill("6");
  await expect(page.locator(".source")).toContainText("ECMWF ensemble (recorded), automatisch gewählt");
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(24);
  expect(requests).toBe(1);
});

test("without a chosen model the finest one for place and days draws, and says so", async ({ page }) => {
  await freezeClock(page);
  await showMeteogram(page, url("braunschweig"));
  const picker = page.getByLabel("Modell");
  await expect(picker).toHaveValue("");
  await expect(picker.locator("option").first()).toHaveText("Automatisch (DWD ICON-EU ensemble (recorded))");
  await expect(page.locator(".source")).toContainText("DWD ICON-EU ensemble (recorded), automatisch gewählt");
  // More days than any model but ECMWF reaches are on offer.
  await expect(page.getByRole("slider")).toHaveAttribute("max", "15");
});

test("search, choose, and come back", async ({ page }) => {
  await freezeClock(page);
  // The geocoder is a third party; answer for it.
  await page.route("**/api/geocode?**", async (route) => {
    const q = new URL(route.request().url()).searchParams.get("q") ?? "";
    const results = q.toLowerCase().startsWith("reyk")
      ? [{ name: "Reykjavík", lat: 64.1355, lon: -21.8954, admin1: "Hauptstadtregion", country: "Island", country_code: "IS" }]
      : [];
    await route.fulfill({ json: { query: q, results } });
  });
  await showMeteogram(page, url("braunschweig"));
  const search = page.getByRole("combobox", { name: "Ort suchen" });
  await search.fill("Reykj");
  await expect(page.getByRole("listbox").getByRole("option")).toHaveText(/Reykjavík.*Island/);
  await search.press("Enter");
  await expect(page.getByTestId("place")).toHaveText("Reykjavík, Island");
  await expect(page).toHaveURL(/lat=64\.1355&lon=-21\.8954&name=Reykjav%C3%ADk%2C\+Island&lang=de/);

  await page.goBack();
  await expect(page.getByTestId("place")).toHaveText("Braunschweig, Germany");
  await page.goForward();
  await expect(page.getByTestId("place")).toHaveText("Reykjavík, Island");
});

test("another model can be chosen, and the link keeps it", async ({ page }) => {
  await freezeClock(page);
  const chart = await showMeteogram(page, url("braunschweig", "&days=10"));
  const picker = page.getByLabel("Modell");
  // Braunschweig: the global models and the two DWD regional ones - not MeteoSwiss.
  await expect(picker.locator("option")).toHaveText([
    "Automatisch (ECMWF ensemble (recorded))", // ten days: only ECMWF reaches that far
    "ECMWF ensemble (recorded) (51 Mitglieder, 25 km, bis 15 Tage)",
    "DWD ICON ensemble (recorded) (40 Mitglieder, 26 km, bis 7,5 Tage)",
    "DWD ICON-EU ensemble (recorded) (40 Mitglieder, 13 km, bis 5 Tage)",
    "DWD ICON-D2 ensemble (recorded) (20 Mitglieder, 2 km, bis 2 Tage)",
  ]);
  await picker.selectOption("icon");
  await expect(page).toHaveURL(/product=icon/);
  await expect(page.locator(".source")).toContainText("DWD ICON ensemble (recorded), 40 Ensemble-Mitglieder");
  await expect(page.locator(".source")).not.toContainText("ECMWF");
  // ICON reaches 7.25 days: the days follow what the model has, a begun one included.
  await expect(page.getByRole("slider")).toHaveAttribute("max", "8");
  await expect(chart.locator("image").first()).toBeAttached();
  await page.goBack();
  await expect(page.locator(".source")).toContainText("ECMWF ensemble (recorded), automatisch gewählt, 51 Ensemble-Mitglieder");
});

test("a regional model is offered where it covers the place", async ({ page }) => {
  await freezeClock(page, "zermatt");
  await showMeteogram(page, url("zermatt"));
  const picker = page.getByLabel("Modell");
  await expect(picker.locator("option")).toHaveCount(6); // "Automatisch" and all five
  // Five days in the Alps: MeteoSwiss, chosen automatically.
  await expect(page.locator(".source")).toContainText("MeteoSwiss ICON-CH2 ensemble (recorded), automatisch gewählt");
  await picker.selectOption("meteoswiss");
  await expect(page.locator(".source")).toContainText("MeteoSwiss ICON-CH2 ensemble (recorded), 21 Ensemble-Mitglieder");
  await expect(page.getByTestId("meteogram").locator("image").first()).toBeAttached();
});

test("a new place the chosen model does not reach goes back to the automatic choice", async ({ page }) => {
  await freezeClock(page);
  await page.route("**/api/geocode?**", async (route) => {
    const q = new URL(route.request().url()).searchParams.get("q") ?? "";
    await route.fulfill({ json: { query: q, results: [
      { name: "Reykjavík", lat: 64.1355, lon: -21.8954, admin1: "Hauptstadtregion", country: "Island", country_code: "IS" },
    ] } });
  });
  await showMeteogram(page, url("braunschweig", "&product=icon-d2"));
  // Hourly, the run reaches 58 hours: a begun third day is offered.
  await expect(page.getByRole("slider")).toHaveAttribute("max", "3");
  const search = page.getByRole("combobox", { name: "Ort suchen" });
  await search.fill("Reykj");
  await expect(page.getByRole("listbox").getByRole("option")).toHaveText(/Reykjavík/);
  await search.press("Enter");
  await expect(page.getByTestId("place")).toHaveText("Reykjavík, Island");
  await expect(page).not.toHaveURL(/product=/);
  await expect(page.locator(".source")).toContainText("DWD ICON-EU ensemble (recorded), automatisch gewählt");
});

test("a model without a forecast for the place offers the automatic choice", async ({ page }) => {
  await freezeClock(page);
  // A link to MeteoSwiss for Braunschweig - outside its area.
  await page.goto(url("braunschweig", "&product=meteoswiss"));
  await expect(page.getByRole("alert")).toContainText("Das gewählte Modell hat für diesen Ort keine Vorhersage.");
  await page.getByRole("button", { name: "Automatisch wählen" }).click();
  await expect(page).not.toHaveURL(/product=/);
  await expect(page.getByTestId("meteogram").locator("image").first()).toBeAttached();
});

test("a model that computes every hour is drawn hourly, and 6-hourly on request", async ({ page }) => {
  await freezeClock(page);
  // Two days in Braunschweig: ICON-D2, 2 km, hourly by its default.
  const chart = await showMeteogram(page, url("braunschweig", "&days=2"));
  await expect(page.locator(".source")).toContainText("DWD ICON-D2 ensemble (recorded), automatisch gewählt");
  const steps = page.getByRole("group", { name: "Schritte", exact: true });
  await expect(steps.getByRole("button", { name: "stündlich" })).toHaveAttribute("aria-pressed", "true");
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(48);
  await expect(chart.locator(".axis .hour").first()).toHaveText("00");
  await expect(chart.locator(".axis .hour").nth(1)).toHaveText("03");
  // The legend shows the hourly rain classes.
  await expect(page.getByRole("region", { name: "Legende" })).toContainText("(mm / 1 h)");

  await steps.getByRole("button", { name: "6 h" }).click();
  await expect(page).toHaveURL(/step=6/);
  // Two days of 6-hour steps, and the run's last one that closes them.
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(9);
  await expect(page.getByRole("region", { name: "Legende" })).toContainText("(mm / 6 h)");
});

test("a model with only 6-hour steps offers no choice of steps", async ({ page }) => {
  await freezeClock(page);
  await showMeteogram(page, url("braunschweig", "&product=ecmwf"));
  await expect(page.getByRole("group", { name: "Schritte", exact: true })).toHaveCount(0);
});

test("a place without data says so", async ({ page }) => {
  await page.goto("/?lat=0&lon=0&lang=de");
  await expect(page.getByRole("alert")).toContainText("Für diesen Ort gibt es keine Vorhersage.");
  await expect(page.getByRole("button", { name: "Erneut versuchen" })).toHaveCount(0);
});

test("the legend comes from the configuration", async ({ page }) => {
  await freezeClock(page);
  await showMeteogram(page, url("braunschweig"));
  const legend = page.getByRole("region", { name: "Legende" });
  await expect(legend.locator("figure")).toHaveCount(3);
  await expect(legend).toContainText("0,1–2 mm");
  await expect(legend).toContainText("≥ 17,2 m/s");
});
