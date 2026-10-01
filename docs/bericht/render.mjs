// Screenshots from the running app and the PDF print, both with Playwright's
// pinned Chromium from webapp/frontend. Run from the repository root:
//
//   (cd webapp && SOURCES_CONFIG=config/sources.fixtures.yaml .venv/bin/python -m api serve --port 8766)
//   node docs/bericht/render.mjs shots     # docs/bericht/img/*.png from the offline app
//   webapp/.venv/bin/python docs/bericht/build.py
//   node docs/bericht/render.mjs pdf       # bericht.html -> Bericht_Probabilistische_Meteogramme.pdf
//
// The app needs a built frontend (webapp/frontend/dist, `npm run build`).
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const require = createRequire(here("../../webapp/frontend/package.json"));
const { chromium, devices } = require("@playwright/test");

const BASE = process.env.APP_URL ?? "http://127.0.0.1:8766";
const FIXTURES = here("../../webapp/tests/fixtures/");
const LOCATIONS = JSON.parse(readFileSync(FIXTURES + "locations.json", "utf8"));

const firstStep = (key) => new Date(JSON.parse(readFileSync(`${FIXTURES}${key}.json`, "utf8")).steps[0]);
const url = (key, days) => {
  const l = LOCATIONS[key];
  return `${BASE}/?lat=${l.latitude}&lon=${l.longitude}&name=${encodeURIComponent(l.name)}&lang=de&days=${days}`;
};

async function shots(browser) {
  // The clock sits on the recording's first step, so every meteogram starts at
  // local midnight of 29.09.2026. The browser's own zone is deliberately
  // another one: the app must draw in the place's time.
  // width: a desktop window this wide, or "phone". pick: what to photograph -
  // the whole meteogram unless given. extra: more URL parameters.
  const shot = async (key, days, width, file, pick = (page, chart) => chart, extra = "") => {
    const screen = width === "phone" ? devices["Pixel 7"] : { viewport: { width, height: 1000 }, deviceScaleFactor: 2 };
    const context = await browser.newContext({ ...screen, locale: "de-DE", timezoneId: "America/New_York" });
    const page = await context.newPage();
    await page.clock.setFixedTime(firstStep(key));
    await page.goto(url(key, days) + extra);
    const chart = page.getByTestId("meteogram");
    await chart.locator("image").first().waitFor({ state: "attached" });
    await page.waitForLoadState("networkidle");
    await pick(page, chart).screenshot({ path: here(`img/${file}`) });
    console.log(file);
    await context.close();
  };
  // A week on a desktop fits in one row; on a phone it scrolls, or runs down the page.
  await shot("braunschweig", 7, 1000, "braunschweig.png");
  await shot("braunschweig", 7, "phone", "braunschweig-phone-row.png");
  await shot("braunschweig", 3, "phone", "braunschweig-phone-column.png", undefined, "&layout=vertical");
  for (const key of ["alice_springs", "singapore", "reykjavik", "zermatt"]) await shot(key, 10, 1240, `${key}.png`);
  await shot("braunschweig", 5, 1000, "legende.png", (page) => page.locator(".legend-schemes"));
}

async function pdf(browser) {
  const page = await browser.newPage();
  await page.goto("file://" + here("bericht.html"));
  await page.waitForLoadState("networkidle");
  await page.pdf({ path: here("../../Bericht_Probabilistische_Meteogramme.pdf"), preferCSSPageSize: true, printBackground: true, tagged: true });
  console.log("Bericht_Probabilistische_Meteogramme.pdf");
}

const browser = await chromium.launch();
try {
  const what = process.argv[2];
  if (what === "shots") await shots(browser);
  else if (what === "pdf") await pdf(browser);
  else throw new Error("usage: render.mjs shots|pdf");
} finally {
  await browser.close();
}
