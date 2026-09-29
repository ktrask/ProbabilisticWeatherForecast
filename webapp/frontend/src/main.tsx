import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import { I18nContext, detectLang, makeI18n } from "./i18n";
import "./styles.css";

const lang = detectLang(window.location.search, navigator.languages);
document.documentElement.lang = lang;
const i18n = makeI18n(lang);
document.title = i18n.t.title;

const queryClient = new QueryClient();

createRoot(document.getElementById("root") as HTMLElement).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <I18nContext.Provider value={i18n}>
        <App />
      </I18nContext.Provider>
    </QueryClientProvider>
  </StrictMode>,
);
