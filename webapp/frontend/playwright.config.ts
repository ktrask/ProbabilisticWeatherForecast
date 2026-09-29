// End-to-end tests against the real backend on recorded forecasts: the API
// runs with config/sources.fixtures.yaml, so there is no network and the same
// answer every time, which is what makes the screenshots comparable.
//
//   npm run e2e                           # compare against e2e/__screenshots__/
//   npm run e2e -- --update-snapshots     # after an intended visual change
//
// PYTHON picks the interpreter for the backend (default: webapp/.venv).
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { defineConfig, devices } from "@playwright/test";

const API_PORT = 8765;
const WEB_PORT = 4174;
const venv = fileURLToPath(new URL("../.venv/bin/python", import.meta.url));
const python = process.env.PYTHON ?? (existsSync(venv) ? venv : "python3");

export default defineConfig({
  testDir: "e2e",
  snapshotPathTemplate: "{testDir}/__screenshots__/{testFileName}/{arg}-{projectName}-{platform}{ext}",
  fullyParallel: true,
  forbidOnly: true,
  reporter: [["list"]],
  expect: { toHaveScreenshot: { maxDiffPixelRatio: 0.002 } },
  use: {
    baseURL: `http://127.0.0.1:${WEB_PORT}`,
    locale: "de-DE",
    // Deliberately not a zone of any fixture: times must follow the forecast's
    // location, never the browser.
    timezoneId: "America/New_York",
    trace: "retain-on-failure",
  },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"], viewport: { width: 1280, height: 900 } }, grepInvert: /@phone/ },
    { name: "phone", use: { ...devices["Pixel 7"] }, grep: /@phone/ },
  ],
  webServer: [
    {
      command: `${python} -m api serve --port ${API_PORT}`,
      cwd: "..",
      env: { SOURCES_CONFIG: "config/sources.fixtures.yaml" },
      url: `http://127.0.0.1:${API_PORT}/api/health`,
      reuseExistingServer: false,
      timeout: 60_000,
    },
    {
      command: `npx vite build && npx vite preview --port ${WEB_PORT}`,
      env: { API_URL: `http://127.0.0.1:${API_PORT}` },
      url: `http://127.0.0.1:${WEB_PORT}`,
      reuseExistingServer: false,
      timeout: 120_000,
    },
  ],
});
