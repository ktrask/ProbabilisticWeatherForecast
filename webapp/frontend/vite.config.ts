import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The API the dev server and the preview server forward to: `python -m api serve`.
const api = process.env.API_URL ?? "http://127.0.0.1:8000";
const proxy = { "/api": api, "/pictograms": api };

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true, proxy },
  preview: { port: 4173, strictPort: true, proxy },
  test: {
    environment: "jsdom",
    setupFiles: ["src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
