# Plan: Neuentwicklung als reaktive Webanwendung

Stand: 28.09.2026 · Ausgangspunkt: Branch `tests-and-cloud-unit-fix` (Commit `865c8ec`)

## 1. Ziel

Die Kernidee bleibt: Die Meteogramme stellen die Streuung des Ensembles über VSUP-Piktogramme und
Perzentil-Bänder dar. Die Architektur wird aber neu aufgebaut:

| Heute | Neu |
|---|---|
| Flask rendert mit matplotlib ein 300-dpi-PNG und bettet es als Base64 in HTML ein | Python-Backend liefert **nur Daten** (JSON), der Browser zeichnet das Meteogramm als SVG |
| Neues Formular-Submit und neues Bild für jede Änderung | Reaktive Oberfläche: Zeitraum, Variante und Hover-Details ändern sich ohne Neuladen und ohne neuen Server-Aufruf |
| VSUP-Regeln fest in `getVSUP*Coordinate()`, positionsgebunden an Dateinamenlisten | VSUP-Schema in einer **Konfigurationsdatei**, beim Start validiert |
| Genau eine Datenquelle, fest in `getData()` verdrahtet | **Quellen-Adapter** mit gemeinsamem Datenmodell; Ensemble und HRES können aus verschiedenen Quellen kommen |
| Wind kommt in km/h, die Schwellen sind in m/s | **Kanonische SI-Einheiten** im gesamten System; jeder Adapter muss sie liefern, die Config deklariert sie |

Ausdrücklich **nicht** Ziel: eine eigene Nutzerverwaltung, Datenbank oder Speicherung von Vorhersagen.
Die Anwendung bleibt zustandslos.

## 2. Zielarchitektur

```
┌──────────────────────── Browser (React + TypeScript) ────────────────────────┐
│  Ortssuche ─► useForecast(lat, lon, source, variant)  ─ TanStack Query ─┐    │
│                                                                         │    │
│  <Meteogram>                                                            │    │
│    <PictogramRow var="cloud">   ◄── pictograms[] aus der API            │    │
│    <PictogramRow var="precip">                                          │    │
│    <QuantileBand var="temperature">  ◄── quantiles{} aus der API        │    │
│    <PictogramRow var="wind">                                            │    │
│    <Crosshair/Tooltip>  zeigt die Perzentile des gewählten Zeitschritts │    │
│  <Legend>  ◄── wird aus /api/schemes generiert, nicht als PNG gepflegt  │    │
└─────────────────────────────────────────────────────────────────────────┼────┘
                                                                          │ JSON
┌──────────────────────── Backend (Python, FastAPI) ───────────────────────▼────┐
│  api/        Routen, Pydantic-Antwortmodelle → OpenAPI                        │
│  sources/    Adapter: open_meteo_ensemble, open_meteo_deterministic, …        │
│  core/       Datenmodell, Einheiten, Reduktion Member→Quantile, Zeitachse     │
│  vsup/       Schema-Loader + Klassifikator (Config → Piktogramm-ID + Stufe)    │
│  config/     sources.yaml, vsup.yaml  (+ JSON-Schema für Editor-Support)      │
└───────────────────────────────────────────────────────────────────────────────┘
       Piktogramme: /pictograms/<scheme>/<id>.svg, statisch ausgeliefert
```

### Wo wird klassifiziert?

Die Auswahl des Piktogramms (Perzentile → Symbol + Stufe) passiert **im Backend**. Der Browser
zeichnet nur. Gründe:

- Es gibt eine einzige Implementierung der Regeln. Sie ist in Python getestet, wie heute
  `tests/test_pictograms.py`.
- Die Config wird beim Serverstart geprüft. Ein Fehler fällt dann beim Deployment auf, nicht erst
  im Browser eines Nutzers.
- Andere Clients (CLI-Export, eine App, ein Bot) bekommen dieselbe Klassifikation.

Die API liefert trotzdem die vollständigen Quantile mit. So kann das Frontend Tooltips zeigen, und
später ließe sich eine clientseitige Vorschau für Config-Änderungen bauen (siehe Phase 6).

"Kein Rendering im Backend" heißt konkret: **matplotlib, Pillow-Rendering und Base64-PNGs
entfallen vollständig.** Das Backend liefert JSON und statische Dateien aus.

## 3. Technologieauswahl

| Bereich | Wahl | Begründung |
|---|---|---|
| Web-Framework | **FastAPI** + Pydantic v2 | Antwortmodelle erzeugen automatisch OpenAPI; async passt zu mehreren parallelen Upstream-Abrufen (Ensemble + HRES) |
| HTTP-Client | `httpx` (async) mit explizitem Timeout pro Client | ersetzt `TimeoutCachedSession`; die Konvention "jeder Outbound-Call hat einen Timeout" bleibt, jetzt direkt über die Client-Konfiguration |
| Upstream-Cache | `hishel` (HTTP-Cache für httpx) oder In-Process-TTL-Cache, 1 h | ersetzt `requests_cache`/`.cache.sqlite`; bei mehreren Workern optional Redis |
| Datenverarbeitung | numpy (pandas nur wo nötig) | die Reduktion Member→Quantile ist reine numpy-Arbeit |
| Config | YAML + Pydantic-Modelle, daraus exportiertes JSON-Schema | YAML ist kommentierbar; das JSON-Schema gibt Autovervollständigung in VS Code/JetBrains |
| Einheiten | kleine eigene Einheitentabelle (oder `pint`) | es gibt nur eine Handvoll Größen; entscheidend ist die Prüfung, nicht eine allgemeine Umrechnung |
| Frontend | **React + TypeScript + Vite** | verbreitet, typsicher, schneller Dev-Server |
| Server-State | **TanStack Query** | Caching, Deduplizierung, Lade-/Fehlerzustände; "reaktiv" ohne eigenen Store |
| Zeichnen | **SVG mit d3-scale / d3-shape** in React-Komponenten | volle Kontrolle über die Piktogramm-Reihen; kein Chart-Framework, das gegen das Layout arbeitet |
| API-Typen | `openapi-typescript` aus dem FastAPI-Schema | Backend und Frontend können nicht unbemerkt auseinanderlaufen |
| Tests | pytest (Backend), Vitest (Komponenten), Playwright (E2E + Screenshot-Vergleich) | |
| Auslieferung | ein Container: gunicorn mit uvicorn-Workern, FastAPI liefert auch das gebaute Frontend aus | wie heute ein Prozess, PID 1, `exec` in `startup.sh` |

## 4. Datenmodell (Nachfolger von `allMeteogramData`)

Das heutige Format hat Schwächen: doppelte Verschachtelung (`['tp']['tp']`), Zeitangaben als
`YYYYMMDD`/`HHMM` plus Stunden-Offsets als Strings, und keine Einheiten. Das neue Modell macht
Einheit, Aggregation und Herkunft explizit:

```jsonc
{
  "location": { "lat": 52.25, "lon": 10.5, "elevation_m": 80, "timezone": "Europe/Berlin",
                "name": "Braunschweig, Germany" },
  "run":      { "source": "open-meteo:ecmwf_ifs025", "init_time": "2026-09-17T22:00:00Z",
                "members": 51 },
  "deterministic_run": { "source": "open-meteo:ecmwf_ifs", "init_time": "2026-09-18T00:00:00Z" },  // optional
  "steps": ["2026-09-18T04:00:00Z", "2026-09-18T10:00:00Z", …],   // ISO 8601, immer UTC
  "step_hours": 6,
  "variables": {
    "temperature_2m": {
      "unit": "degC", "kind": "instant",
      "quantiles": { "p0": […], "p10": […], "p25": […], "p50": […], "p75": […], "p90": […], "p100": […] },
      "deterministic": […]            // nur wenn eine deterministische Quelle angebunden ist
    },
    "precipitation": { "unit": "mm", "kind": "sum", "window_hours": 6, "quantiles": {…} },
    "cloud_cover":   { "unit": "percent", "kind": "instant", "quantiles": {…} },
    "wind_speed_10m":{ "unit": "m/s",  "kind": "instant", "quantiles": {…} }
  },
  "pictograms": {
    "cloud_cover": { "scheme": "cloud-vsup@3", "items": [ { "id": "cloud/clear", "level": 3, "class": "clear" }, … ] },
    …
  }
}
```

Regeln, die aus dem bestehenden Code übernommen werden:

- **Summen-Variablen werden pro Member aggregiert, bevor Quantile gebildet werden.** Das ist die
  Logik von `accumulate_over_steps()`. Sie wird generisch über `kind: sum` gesteuert, sodass jede
  neue Summen-Variable (z. B. Schnee oder Sonnenscheindauer) automatisch richtig behandelt wird.
- Die Quantil-Stufen sind konfigurierbar (Standard: 0/10/25/50/75/90/100). Die VSUP-Regeln dürfen
  nur Quantile verwenden, die auch berechnet werden; das prüft der Config-Validator.
- Zeiten sind im Modell immer UTC und tz-aware. Die Umrechnung in Ortszeit macht erst das Frontend
  (`Intl.DateTimeFormat` mit `location.timezone`). Damit entfällt die naive `utcNow()`-Hilfskonstruktion.
- Das Frontend schneidet den Zeitraum selbst zu. Die API liefert immer den vollen Horizont der Quelle.
  Damit ist der heutige Fehler behoben, dass Meteogramme fest beim zweiten Schritt beginnen statt
  bei "jetzt" (`fromIndex`).

`tests/schema.py` wird zum Pydantic-Modell. Offline-Fixtures und der Live-Test prüfen weiterhin
gegen genau eine Definition.

## 5. Einheiten: Wind korrekt von Anfang an

Kanonische Einheiten im ganzen System:

| Größe | Einheit | Bemerkung |
|---|---|---|
| Temperatur | `degC` | |
| Niederschlag | `mm` pro Fenster (`window_hours`) | Schwellen beziehen sich auf das Fenster; ändert sich das Fenster, muss die Config es ausdrücklich ebenfalls ändern |
| Bewölkung | `percent` (0–100) | |
| Wind | **`m/s`** | Beaufort-Grenzen sind in m/s definiert |

Umsetzung auf drei Ebenen, damit sich der Fehler nicht wiederholen kann:

1. **An der Quelle anfordern:** Open-Meteo unterstützt `wind_speed_unit=ms`. Am 28.09.2026 gegen
   die Ensemble-API geprüft: `hourly_units` meldet dann `"m/s"` für alle Member.
2. **Adapter prüfen die gemeldete Einheit:** Jeder Adapter liest die Einheit aus der Antwort
   (`hourly_units` bzw. die SDK-Metadaten) und rechnet in die kanonische Einheit um. Eine
   unbekannte Einheit ist ein harter Fehler, kein stilles Weiterreichen.
3. **Die Config deklariert ihre Einheit:** Jede Variable im VSUP-Schema trägt `unit: m/s`. Der
   Loader lehnt die Config ab, wenn diese Einheit nicht der kanonischen entspricht. Wer später
   Schwellen in km/h schreiben will, muss das ausdrücklich deklarieren und bekommt eine
   Umrechnung beim Laden, nie zur Laufzeit.

Die bisherigen Schwellen 3 / 10 / 17,2 m/s bleiben der Startpunkt. 17,2 m/s ist die Untergrenze
von Beaufort 8. 3 und 10 sind gerundete Werte; bei Bedarf lassen sie sich über die Config auf die
exakten Beaufort-Grenzen (3,4 und 10,8 m/s) setzen, ohne Code zu ändern. Der heutige xfail
`TestUnitMismatch` wird im neuen System zu einem regulären Test: *"4,8 m/s wird als leichter Wind
klassifiziert."*

## 6. VSUP-Konfiguration

### 6.1 Anforderungen

- Neue Piktogramme hinzufügen, ohne Python-Code zu ändern.
- Anzahl der Stufen (heute 3 bei VSUP, 4 bei HRES) und Anzahl der Intensitätsklassen frei wählbar.
- Klassengrenzen und die Quantile, die über die Sicherheit entscheiden, frei wählbar.
- Mehrere Schemata nebeneinander (z. B. `ensemble` und `hres-enhanced`), auswählbar pro Anfrage.
- Die heutige Logik muss sich **exakt** abbilden lassen, einschließlich ihrer Eigenheiten.
- Fehler werden beim Laden gefunden, nicht beim ersten Request.

### 6.2 Zwei Modi

**Modus `tree` (Standard für neue Schemata)** beschreibt VSUP so, wie es gemeint ist: Intensitätsklassen,
die mit sinkender Sicherheit zu Gruppen verschmelzen. Die Stufe ergibt sich daraus, auf welcher Ebene
ein Quantil-Intervall noch vollständig in *einer* Gruppe liegt:

```yaml
# config/vsup.yaml
version: 1
quantiles: [0, 10, 25, 50, 75, 90, 100]

schemes:
  precipitation-vsup:
    variable: precipitation
    unit: mm               # pro 6-h-Fenster; muss zur kanonischen Einheit passen
    window_hours: 6
    mode: tree
    classes:               # aufsteigend; die letzte Klasse ist nach oben offen
      - { id: none,   below: 0.1 }
      - { id: light,  below: 1.0 }
      - { id: medium, below: 2.0 }
      - { id: heavy }
    levels:                # von sicher nach unsicher; die erste zutreffende Ebene gewinnt
      - level: 3
        interval: [p10, p90]           # 80 % der Member in derselben Gruppe …
        groups: [[none], [light], [medium], [heavy]]
      - level: 2
        interval: [p25, p75]           # … sonst die mittlere Hälfte
        groups: [[none, light], [medium, heavy]]
      - level: 1
        groups: [[none, light, medium, heavy]]   # immer erfüllt
    pictograms:            # Schlüssel = Stufe:Gruppe; Datei relativ zu pictograms/
      "3:none":            rain/dry.svg
      "3:light":           rain/light.svg
      "3:medium":          rain/medium.svg
      "3:heavy":           rain/heavy.svg
      "2:none+light":      rain/mostly-dry.svg
      "2:medium+heavy":    rain/mostly-rainy.svg
      "1:none+light+medium+heavy": rain/unknown.svg
```

Eine zusätzliche Stufe oder ein fünftes Piktogramm kommt als neuer Eintrag in `levels` bzw. `classes`
dazu, plus die Zuordnung in `pictograms`.

**Modus `rules` (für exakte Altlogik und Sonderfälle)** ist eine geordnete Regelliste; die erste
zutreffende Regel gewinnt, genau wie die heutigen `if`-Ketten. Damit lässt sich z. B.
`getVSUPrainCoordinate` inklusive der 1,5-mm-Schwelle und der Regelreihenfolge 1:1 übertragen:

```yaml
  precipitation-legacy:
    variable: precipitation
    unit: mm
    window_hours: 6
    mode: rules
    rules:
      - { when: "p90 < 0.1",               pictogram: rain/Stufe3_KeinRegen.svg,      level: 3, class: none }
      - { when: "p10 > 2",                 pictogram: rain/Stufe3_Starkregen.svg,     level: 3, class: heavy }
      - { when: "p10 > 1 and p90 < 2",     pictogram: rain/Stufe3_MittlererRegen.svg, level: 3, class: medium }
      - { when: "p10 > 1",                 pictogram: rain/Stufe2_Regen.svg,          level: 2, class: medium+heavy }
      - { when: "p90 < 1",                 pictogram: rain/Stufe3_leichterRegen.svg,  level: 3, class: light }
      - { when: "p50 > 1",                 pictogram: rain/Stufe2_Regen.svg,          level: 2, class: medium+heavy }
      - { when: "p75 < 1.5",               pictogram: rain/Stufe2_KaumRegen.svg,      level: 2, class: none+light }
      - { default: true,                   pictogram: rain/step1.svg,                 level: 1, class: any }
```

Die `when`-Ausdrücke werden mit einem **kleinen eigenen Parser** ausgewertet, nicht mit `eval`.
Erlaubt sind nur Quantil-Namen, `deterministic`, Zahlen, `< <= > >=`, `and`, `or`, `not` und
Klammern. Der Parser gibt eine verständliche Fehlermeldung mit Zeilennummer aus.

**HRES-Schemata** nutzen einen dritten Baustein: `class_from: deterministic`. Die Klasse kommt aus
dem deterministischen Wert, die Stufe daraus, welcher Anteil der Member auf derselben Seite der
Klassengrenze liegt (heute: p90 → Stufe 4, p50 → 3, p25 → 2, sonst 1). Das ist die Logik von
`getHres*Coordinate()`, ebenfalls als Config:

```yaml
  precipitation-hres:
    variable: precipitation
    unit: mm
    window_hours: 6
    mode: anchored
    class_from: deterministic
    classes: *precip_classes           # YAML-Anker, gleiche Grenzen wie oben
    agreement:                         # erste zutreffende gewinnt
      - { level: 4, members_agree: p90 }   # ≥ 90 % der Member in derselben Klasse
      - { level: 3, members_agree: p50 }
      - { level: 2, members_agree: p25 }
      - { level: 1 }
    pictograms: "rain/hres/{level}_{class}.svg"   # Muster statt 16 Einzeleinträge
```

Zu klären ist, ob die heutige HRES-Logik bei den mittleren Klassen nur die jeweils *eine* relevante
Grenze prüft (so steht es im Code) oder beide. Der Modus bildet zunächst den Ist-Zustand ab, per
Option `agreement_bounds: nearest | both`.

### 6.3 Validierung beim Laden

Der Loader bricht den Start ab (und die CI schlägt fehl), wenn:

- eine referenzierte Piktogrammdatei fehlt,
- Klassengrenzen nicht streng aufsteigend sind,
- eine Regel ein Quantil verwendet, das nicht berechnet wird,
- `unit` nicht zur kanonischen Einheit der Variable passt (siehe Abschnitt 5),
- im `tree`-Modus eine Kombination aus Stufe und Gruppe kein Piktogramm hat,
- im `rules`-Modus die letzte Regel kein `default` ist (sonst gäbe es Lücken),
- ein Schema eine deterministische Reihe braucht (`class_from: deterministic`), die keine
  konfigurierte Quelle liefert. Das ist der Nachfolger von `HresDataUnavailable`, jetzt als
  Fähigkeitsprüfung beim Start und nicht erst im Request.

Zusätzlich gibt es ein CLI `python -m vsup check config/vsup.yaml`. Es erzeugt außerdem eine
**Abdeckungstabelle**: welche Piktogramme auf den Test-Fixtures wie oft gewählt werden. So fallen
unerreichbare Regeln sofort auf.

### 6.4 Piktogramm-Assets

- Einheitlich **SVG**. Für die Ensemble-Piktogramme liegen SVGs schon vor. Die 48 HRES-Piktogramme
  (`*/enhanced_hres/`) gibt es nur als PNG. Sie müssen neu als SVG erzeugt werden; da sie sich nur in
  der Farbe unterscheiden, reicht wahrscheinlich eine SVG pro Klasse, eingefärbt per CSS
  (`currentColor`) oder per Config (`color:` pro Stufe).
- Die Farbe pro Stufe gehört in die Config (`level_styles: {3: {color: "#004e5e"}, …}`), nicht in
  die Grafikdatei. Dann lassen sich Farbschema und Dark Mode ohne neue Grafiken anpassen.
- Die Legende (heute `vsup_all.png`, von Hand gepflegt) erzeugt das Frontend aus dem Schema. Sie
  stimmt damit immer mit den tatsächlichen Regeln überein.

## 7. Datenquellen

### 7.1 Adapter-Schnittstelle

```python
class ForecastSource(Protocol):
    id: str                               # "open-meteo:ecmwf_ifs025"
    kind: Literal["ensemble", "deterministic", "quantiles"]
    variables: set[Variable]              # was die Quelle liefern kann
    max_lead: timedelta
    native_step: timedelta

    async def fetch(self, loc: Location, variables: set[Variable]) -> SourceResult: ...
```

`SourceResult` enthält entweder Member-Arrays (`kind=ensemble`), eine einzelne Reihe
(`deterministic`) oder fertige Quantile (für Quellen, die nur solche veröffentlichen). Alle Werte
sind bereits in kanonischen Einheiten, alle Zeiten UTC.

Die gemeinsame Pipeline übernimmt dann:

1. Zeitachsen angleichen. Ensemble und HRES haben unterschiedliche Laufzeiten und Schrittweiten.
   Beide werden auf das gemeinsame 6-h-Raster gebracht; dabei werden Summen-Variablen summiert
   und Momentanwerte abgetastet.
2. Member → Quantile, mit Pro-Member-Summierung vor der Quantilbildung.
3. VSUP-Klassifikation pro Variable und Schema.

### 7.2 Quellen-Konfiguration

```yaml
# config/sources.yaml
sources:
  ecmwf-ens:
    adapter: open_meteo_ensemble
    model: ecmwf_ifs025
    timeout_s: 30
  ecmwf-hres:
    adapter: open_meteo_forecast
    model: ecmwf_ifs              # deterministisch, 10 Tage; am 28.09.2026 verfügbar geprüft
    timeout_s: 30
  icon-eps:
    adapter: open_meteo_ensemble
    model: icon_seamless          # DWD ICON-Ensemble, ebenfalls über Open-Meteo abrufbar

products:                        # was die UI zur Auswahl anbietet
  ecmwf:
    label: "ECMWF-Ensemble"
    ensemble: ecmwf-ens
    deterministic: ecmwf-hres    # optional; ohne Eintrag wird die HRES-Variante in der UI ausgeblendet
    schemes: [precipitation-vsup, cloud-vsup, wind-vsup]
    hres_schemes: [precipitation-hres, cloud-hres, wind-hres]
  dwd:
    label: "DWD ICON-Ensemble"
    ensemble: icon-eps
    schemes: [precipitation-vsup, cloud-vsup, wind-vsup]
```

Ein Produkt, das keine deterministische Quelle hat, bietet die HRES-Variante in der UI gar nicht
erst an. Heute wird der Radio-Button angezeigt und das Backend antwortet mit 503; das entfällt.

### 7.3 Erste Adapter

| Adapter | Zweck | Anmerkung |
|---|---|---|
| `open_meteo_ensemble` | ECMWF IFS 0,25° (51 Member), ICON-EPS, GEFS … | Nachfolger von `getData()`; fordert `wind_speed_unit=ms` an |
| `open_meteo_forecast` | deterministische Läufe (`ecmwf_ifs`, `ecmwf_ifs025`, `icon_seamless`) | liefert die `deterministic`-Reihe für HRES-Schemata. Vor der Umsetzung klären, welches Produkt `ecmwf_ifs` bei Open-Meteo genau ist (Auflösung, Initialisierungszeit) |
| `fixture` | liest `tests/fixtures/*.json` | für Offline-Entwicklung, Tests und Demos, ersetzt `allmeteogramdata.json` |
| später: `ecmwf_opendata` | direkt ECMWF Open Data (GRIB) | nur falls Open-Meteo nicht reicht; braucht `eccodes`, deutlich mehr Betriebsaufwand |

Geocoding (Nominatim) und Zeitzonen (`timezonefinder`) werden eigene kleine Dienste hinter
Schnittstellen. Open-Meteo liefert Höhe und Zeitzone schon mit (`timezone=auto`); damit entfällt
eine Abhängigkeit.

## 8. API

| Methode & Pfad | Antwort |
|---|---|
| `GET /api/geocode?q=Braunschweig` | Trefferliste `{name, lat, lon}`; leer statt Fehler, wenn nichts gefunden wird |
| `GET /api/products` | verfügbare Produkte und Varianten, abgeleitet aus `sources.yaml` + Fähigkeitsprüfung |
| `GET /api/schemes` | die geladenen VSUP-Schemata (für die Legende), inkl. Versions-Hash |
| `GET /api/forecast?lat=&lon=&product=ecmwf&variant=ensemble` | Datenmodell aus Abschnitt 4 |
| `GET /api/health` | Liveness; `?deep=1` prüft zusätzlich die Upstream-Erreichbarkeit |
| `GET /pictograms/...` | statische SVGs mit Cache-Headern und Hash im Pfad |

Fehlerverhalten (die bestehenden Konventionen bleiben erhalten):

- ungültige Parameter → 422 mit Feldangabe (Pydantic erledigt das von selbst; der Umweg über
  `searchForm(request.args)` und `form.validate()` entfällt)
- Ort nicht auflösbar → 404 mit dem eingegebenen Namen
- Upstream-Timeout oder -Fehler → 502/504 mit verständlicher Meldung; keine Wiederholungsschleife ohne Ende
- Variante für dieses Produkt nicht verfügbar → 409 (tritt nur auf, wenn die UI umgangen wird)

Die API ist rein lesend (GET). CSRF bleibt daher unnötig, und es gibt keinen `SECRET_KEY` mehr,
weil es keine Sessions gibt. Der Test in `test_deployment.py`, der schreibende Methoden verbietet,
wird übernommen.

## 9. Frontend

### Aufbau

```
frontend/src/
  api/          generierte Typen (openapi-typescript) + fetch-Hooks (TanStack Query)
  state/        URL-Parameter als einzige Quelle für Ort/Produkt/Variante/Zeitraum
  meteogram/
    Meteogram.tsx        gemeinsame Zeitachse (d3-scale), Layout der Reihen
    PictogramRow.tsx     ein <image>/<use> pro Zeitschritt, Größe hängt von der Breite ab
    QuantileBand.tsx     drei Flächen + Medianlinie (d3-shape area/line)
    ExtremaMarkers.tsx   Tageshöchst- und -tiefstwerte
    DayShading.tsx       Tag/Nacht bzw. jeden zweiten Tag, in Ortszeit
    Crosshair.tsx        Hover/Touch: alle Quantile dieses Schritts, Klasse und Stufe
  legend/       Legende aus /api/schemes
  search/       Ortssuche mit Debounce, Koordinaten-Eingabe, "mein Standort"
```

### Verhalten

- **Der Zustand steckt in der URL** (`?lat=52.26&lon=10.52&product=ecmwf&variant=ensemble&days=5`).
  Links sind teilbar, und Zurück/Vor im Browser funktionieren.
- **Zeitraum und Variante ändern ohne neuen Server-Aufruf:** Die API liefert den vollen Horizont;
  der Tage-Regler schneidet nur clientseitig zu. Ein Wechsel zwischen `ensemble` und `hres` lädt
  nur, wenn die deterministische Reihe noch fehlt.
- **Responsive:** Unterhalb einer Mindestbreite pro Symbol werden die Piktogramme zu 12-h-Schritten
  zusammengefasst. Das passiert auf dem Server über einen `step_hours`-Parameter, damit die
  Klassifikation für das gröbere Fenster korrekt neu gerechnet wird und nicht einfach Symbole
  weggelassen werden.
- **Export:** SVG direkt aus dem DOM; PNG per Canvas im Browser. Ein serverseitiger PNG-Export ist
  nicht nötig.
- **Barrierefreiheit:** Jedes Piktogramm bekommt einen `<title>` ("Sicher leichter Regen,
  0,3–0,8 mm"), Tastaturnavigation über die Zeitschritte, und die Stufen werden neben der Farbe
  auch über die Form unterschieden (das leistet VSUP schon von sich aus).
- **i18n:** Texte in Deutsch und Englisch; Wochentage und Uhrzeiten über `Intl`.

## 10. Migrationsplan

Jede Phase endet mit etwas Lauffähigem. Die alte Flask-App läuft weiter, bis Phase 4
abgeschlossen ist.

### Phase 0 – Wind im Bestand korrigieren (klein, sofort)
- Im heutigen `getData()` `wind_speed_unit=ms` anfordern. Die Alternative laut CLAUDE.md wäre, die
  Schwellen in km/h umzuschreiben; für die Neuentwicklung ist m/s aber die kanonische Einheit,
  daher hier schon die Daten umstellen.
- `TestUnitMismatch` von xfail auf einen regulären Test umstellen; Fixtures neu erzeugen.
- Ergebnis: Die Vergleichsbasis für die Parity-Tests der Neuentwicklung ist fachlich korrekt.

### Phase 1 – Kern extrahieren (Backend, ohne Web)
- Pakete `core/`, `sources/`, `vsup/` neben dem bestehenden `meteogram/`.
- Datenmodell (Pydantic), Einheitenprüfung, Reduktion Member→Quantile, `open_meteo_ensemble`-
  und `fixture`-Adapter.
- VSUP-Loader mit den Modi `rules` und `tree`, Validator, CLI `vsup check`.
- **Golden-Tests:** Die Legacy-Config im Modus `rules` muss auf allen fünf Fixtures für jeden
  Zeitschritt exakt dieselben Piktogramme liefern wie die alten `getVSUP*Coordinate()`-Funktionen
  (beim Wind auf den m/s-Daten aus Phase 0). Erst wenn das grün ist, folgt ein `tree`-Schema, das
  bewusst abweichen darf.

### Phase 2 – API
- FastAPI-App mit den Endpunkten aus Abschnitt 8, httpx mit Timeouts, Upstream-Cache.
- Live-Test (`pytest -m live`) prüft das neue Datenmodell gegen Open-Meteo.
- Contract-Test: Das erzeugte OpenAPI-Schema ist eingecheckt, und Änderungen daran sind im Diff
  sichtbar.

### Phase 3 – Frontend-MVP
- Ortssuche, Meteogramm mit vier Reihen, Legende aus der Config, URL-Zustand, Hover.
- Playwright-Screenshots für die fünf Fixture-Orte (Adapter `fixture`, daher deterministisch).

### Phase 4 – Umstellung
- Ein Container liefert Frontend und API aus; `startup.sh` startet gunicorn mit uvicorn-Workern.
- Sichtprüfung neu gegen alt anhand der Fixtures (die PNGs aus dem Bericht dienen als Referenz).
- Danach entfernen: Flask, flask_wtf, flask_bootstrap, matplotlib, `app/`, `plotMeteogram.py`,
  `allmeteogramdata.json`, die PNG-Legenden, die Root-`requirements.txt` und `plotly.html`.
  Außerdem den toten 15-Tage-Zweig.

### Phase 5 – HRES und weitere Quellen
- `open_meteo_forecast`-Adapter, Zeitachsen-Angleichung Ensemble/HRES, HRES-Schemata im Modus
  `anchored`, 48 HRES-Piktogramme als SVG.
- Zweites Produkt (z. B. DWD ICON-EPS) nur über `sources.yaml`. Das dient als Beleg, dass eine
  neue Quelle ohne Codeänderung außerhalb eines Adapters möglich ist.

### Phase 6 – Ausbau (optional)
- Config-Playground: Schema im Browser bearbeiten und live auf die aktuelle Vorhersage anwenden.
  Dafür bräuchte es eine zweite Implementierung des Klassifikators in TypeScript. Beide müssten gegen
  dieselben Golden-Fixtures getestet werden, oder der Klassifikator läuft als WebAssembly (Pyodide).
- Weitere Variablen: Schnee, Böen, Gewitterwahrscheinlichkeit (CAPE), jeweils nur Config +
  Piktogramme, solange die Quelle sie liefert.
- Mehrere Orte nebeneinander vergleichen.

## 11. Teststrategie

| Ebene | Was | Werkzeug |
|---|---|---|
| Einheiten | Adapter liefern kanonische Einheiten; Config mit falscher Einheit wird abgelehnt; 4,8 m/s ist "leichter Wind" | pytest |
| Aggregation | Summen pro Member vor Quantilen (bestehende `TestPercentilesOfSums` übernehmen) | pytest |
| VSUP | Golden-Tests Legacy-Parität; Grenzwerte jeder Schwelle (knapp darunter / darauf / darüber); Validator-Fehlerfälle | pytest |
| API | Statuscodes, Schema, keine schreibenden Methoden, Timeouts greifen (bestehende `test_resilience.py` übertragen) | pytest + httpx `MockTransport` |
| Live | Open-Meteo-Format und -Einheiten unverändert | `pytest -m live` |
| Frontend | Komponenten (Band, Reihe, Legende) | Vitest + Testing Library |
| E2E | Suche → Meteogramm, URL-Zustand, Screenshots der fünf Fixtures | Playwright |

Die bewährten Konventionen bleiben: Datums-Werte in Tests kommen aus der Fixture selbst und nie aus
`now()`, bekannte Fehler werden als `strict=True`-xfail festgehalten, und es gibt genau eine
Schema-Definition.

Neu: eine **CI-Pipeline** (heute gibt es keine). Mindestens `pytest -m "not live"`,
`vsup check`, Typecheck (mypy/pyright, `tsc`) und Frontend-Tests bei jedem Push.

## 12. Risiken und offene Entscheidungen

| Punkt | Empfehlung | Offen für |
|---|---|---|
| Klassifikation im Backend oder im Browser | Backend (Abschnitt 2) | Umkehr nur, falls der Config-Playground Priorität bekommt |
| Config-Format | YAML + JSON-Schema | TOML wäre möglich, kann aber die verschachtelten Regeln schlechter ausdrücken |
| Ausdruckssprache in `rules` | eigener Mini-Parser | Alternative: strukturierte Bedingungen (`{q: p90, op: "<", value: 0.1}`); robuster, aber schwerer zu lesen |
| HRES-Quelle | Open-Meteo `ecmwf_ifs` | Produktdetails prüfen; ECMWF Open Data direkt nur bei Bedarf |
| Schwellen 3 / 10 m/s oder Beaufort-exakt 3,4 / 10,8 m/s | zunächst unverändert (Parität), später per Config | fachliche Entscheidung |
| Harte Klassengrenzen führen zu Symbolsprüngen | vorerst beibehalten | später eventuell Hysterese oder Unschärfebereich im `tree`-Modus |
| Open-Meteo-Rate-Limits bei öffentlichem Betrieb | Cache 1 h, Anfragen pro Ort deduplizieren | kommerzieller API-Key, falls die Nutzung wächst |
| Aufwand für SVG-Neuzeichnung der HRES-Piktogramme | eine SVG pro Klasse, Farbe per CSS | Designarbeit, nicht Code |

## 13. Grobe Aufwandsschätzung

Für eine Person mit Python- und React-Erfahrung:

| Phase | Aufwand |
|---|---|
| 0 Wind-Fix im Bestand | 0,5 Tage |
| 1 Kern + VSUP-Config + Golden-Tests | 5–7 Tage |
| 2 API | 2–3 Tage |
| 3 Frontend-MVP | 6–8 Tage |
| 4 Umstellung + Aufräumen | 2 Tage |
| 5 HRES + zweite Quelle | 4–6 Tage (+ Designzeit für SVGs) |
| **Summe bis Phase 5** | **ca. 4–5 Wochen** |
