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
  const chart = await showMeteogram(page, url("braunschweig"));
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

test("changing the days needs no new request", async ({ page }) => {
  await freezeClock(page);
  const chart = await showMeteogram(page, url("braunschweig", "&days=2"));
  let requests = 0;
  page.on("request", (request) => {
    if (request.url().includes("/api/forecast")) requests++;
  });
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(8);
  await page.getByRole("slider").fill("6");
  await expect(chart.locator('g[data-variable="cloud_cover"] image')).toHaveCount(24);
  await expect(page).toHaveURL(/days=6/);
  expect(requests).toBe(0);
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
  await expect(picker.locator("option")).toHaveText([
    "ECMWF ensemble (recorded) (51 Mitglieder, 25 km, bis 15 Tage)",
    "DWD ICON ensemble (recorded) (40 Mitglieder, 26 km, bis 7,5 Tage)",
  ]);
  await picker.selectOption("icon");
  await expect(page).toHaveURL(/product=icon/);
  await expect(page.locator(".source")).toContainText("DWD ICON ensemble (recorded), 40 Ensemble-Mitglieder");
  await expect(page.locator(".source")).not.toContainText("ECMWF");
  // ICON reaches about a week: the days follow what the model has.
  await expect(page.getByRole("slider")).toHaveAttribute("max", "7");
  await expect(chart.locator("image").first()).toBeAttached();
  await page.goBack();
  await expect(page.locator(".source")).toContainText("ECMWF ensemble (recorded), 51 Ensemble-Mitglieder");
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
  await expect(legend).toContainText("0,1–1 mm");
  await expect(legend).toContainText("≥ 17,2 m/s");
});
