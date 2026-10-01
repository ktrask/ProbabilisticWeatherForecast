# Plan: Neuentwicklung als reaktive Webanwendung

Stand: 01.10.2026 · Ursprünglicher Plan vom 28.09.2026, seitdem mit dem Umsetzungsstand
fortgeschrieben. Die Phasen 0–4 sind in `main` (Fast-Forward am 29.09.2026). Der letzte Stand der
alten Flask-App liegt auf dem Branch `old-webapp` (`dc7984f`, nach Phase 0 mit allen Tests und
Korrekturen, Dockerfile startet noch Flask).

## 0. Umsetzungsstand

| Phase | Stand | Commits |
|---|---|---|
| 0 Wind im Bestand korrigieren | erledigt | `dc7984f` |
| 1 Kern extrahieren | erledigt | `045b77f`, `dbad5fe` |
| 2 API | erledigt | `cb10270`, `6f15f5f` |
| 3 Frontend-MVP | erledigt | `765b45a`, `d0503b4` |
| 4 Umstellung | erledigt | `862f4c7`, `e1bf118`, `497e098`, `af90976` |
| 5 HRES und weitere Quellen | teilweise: weitere Quellen erledigt (`docs/modelle-plan.md`), HRES offen | `eb96d91` … `51ab7ed` |
| 6 Ausbau | offen, optional | – |

Die alte Flask-/matplotlib-Anwendung ist entfernt. Der Container liefert nur noch die neue App aus
und ist am 29.09.2026 gebaut, gestartet und über HTTP und im Browser geprüft worden.

Wo die Umsetzung vom ursprünglichen Plan abweicht (Begründung jeweils im Abschnitt):

- **Geocoding über Open-Meteo statt Nominatim** (Abschnitt 7.3). Nominatim verbietet eine Suche
  während des Tippens.
- **Niederschlagsfenster eine Stunde später als im Altcode** (Abschnitt 4). Open-Meteo liefert pro
  Stunde die Summe der *vorangehenden* Stunde; der Altcode lag daneben.
- **Schmale Bildschirme: eine scrollbare Zeile oder eine senkrechte Darstellung** statt einer
  12-h-Zusammenfassung über einen `step_hours`-Parameter (Abschnitt 9).
- **Upstream-Cache im Prozess** statt `hishel`, **TypeScript 5.9** statt 7 (Abschnitt 3).
- **Legacy-Regeln als eingefrorene Testreferenz**; die Fixtures liegen inzwischen im
  `Forecast`-Format vor, eine alte Datei bleibt als Muster (Abschnitt 10, Phase 4).
- **„Sicher“ heißt: mindestens zwei Drittel der Member in einer Klasse** (p17–p83), entschieden am
  29.09.2026 (Abschnitt 6.2 und 12).

## 1. Ziel

Die Kernidee bleibt: Die Meteogramme stellen die Streuung des Ensembles über VSUP-Piktogramme und
Perzentil-Bänder dar. Die Architektur ist neu aufgebaut:

| Vorher (bis Phase 4) | Jetzt |
|---|---|
| Flask rendert mit matplotlib ein 300-dpi-PNG und bettet es als Base64 in HTML ein | Python-Backend liefert **nur Daten** (JSON), der Browser zeichnet das Meteogramm als SVG |
| Neues Formular-Submit und neues Bild für jede Änderung | Reaktive Oberfläche: Zeitraum und Hover-Details ändern sich ohne Neuladen und ohne neuen Server-Aufruf |
| VSUP-Regeln fest in `getVSUP*Coordinate()`, positionsgebunden an Dateinamenlisten | VSUP-Schema in einer **Konfigurationsdatei**, beim Start validiert |
| Genau eine Datenquelle, fest in `getData()` verdrahtet | **Quellen-Adapter** mit gemeinsamem Datenmodell; Produkte in `sources.yaml` |
| Wind kam in km/h, die Schwellen waren in m/s | **Kanonische SI-Einheiten** im gesamten System; jeder Adapter liefert sie, die Config deklariert sie |

Offen ist nur der letzte Teil des vierten Punkts: Ensemble und HRES aus verschiedenen Quellen
(Phase 5).

Ausdrücklich **nicht** Ziel: eine eigene Nutzerverwaltung, Datenbank oder Speicherung von
Vorhersagen. Die Anwendung ist zustandslos.

## 2. Architektur

```
┌──────────────────────── Browser (React + TypeScript) ────────────────────────┐
│  Ortssuche ─► useForecast(lat, lon, product, variant) ─ TanStack Query ─┐    │
│                                                                         │    │
│  <Meteogram>  (Zeile, bei Bedarf seitlich scrollbar, oder senkrecht)    │    │
│    Wolken-Piktogramme            ◄── pictograms{} aus der API           │    │
│    Niederschlags-Piktogramme                                            │    │
│    Temperatur-Quantilband        ◄── quantiles{} aus der API            │    │
│    Wind-Piktogramme                                                     │    │
│    Crosshair + Tooltip: alle Quantile, Klasse und Stufe des Schritts    │    │
│  <Legend>  ◄── aus /api/schemes erzeugt, nicht als PNG gepflegt         │    │
└─────────────────────────────────────────────────────────────────────────┼────┘
                                                                          │ JSON
┌──────────────────────── Backend (Python, FastAPI) ───────────────────────▼────┐
│  api/        Routen, Antwortmodelle → OpenAPI; liefert auch das Frontend aus  │
│  sources/    Adapter: open_meteo (Ensemble), fixture, Geocoder; Produkte      │
│  core/       Datenmodell, Einheiten, Reduktion Member→Quantile, Zeitachse     │
│  vsup/       Schema-Loader + Klassifikator (Config → Piktogramm + Stufe)       │
│  config/     sources.yaml, vsup.yaml (+ JSON-Schemata für Editor-Support)     │
└───────────────────────────────────────────────────────────────────────────────┘
       Piktogramme: /pictograms/<version>/<pfad>, statisch, dauerhaft cachebar
       Frontend:    /  (gebautes Vite-Bundle, index.html ohne Cache)
```

Alles liegt unter `webapp/`. Schichten: `core` ← `sources`, `vsup` ← `api`.

### Wo wird klassifiziert?

Die Auswahl des Piktogramms (Perzentile → Symbol + Stufe) passiert **im Backend**. Der Browser
zeichnet nur. Gründe:

- Es gibt eine einzige Implementierung der Regeln, in Python getestet.
- Die Config wird beim Serverstart geprüft. Ein Fehler fällt beim Deployment auf, nicht erst im
  Browser eines Nutzers.
- Andere Clients (CLI-Export, eine App, ein Bot) bekommen dieselbe Klassifikation.

Die API liefert trotzdem die vollständigen Quantile mit. So zeigt der Tooltip alle Werte, und
später ließe sich eine clientseitige Vorschau für Config-Änderungen bauen (Phase 6).

**Kein Rendering im Backend:** matplotlib, Pillow-Rendering und Base64-PNGs sind entfallen. Das
Backend liefert JSON und statische Dateien aus.

## 3. Technologieauswahl

| Bereich | Umgesetzt | Anmerkung |
|---|---|---|
| Web-Framework | **FastAPI** 0.141 + Pydantic 2.13 | Antwortmodelle erzeugen das OpenAPI-Schema |
| HTTP-Client | `httpx` (async), Timeout an **jeder** Anfrage | auch ein geteilter Client ohne eigenen Timeout kann ihn nicht aushebeln |
| Upstream-Cache | eigener In-Process-TTL-Cache (`api/cache.py`) | Vorhersagen 1 h, Geocoding 1 Tag; gleichzeitige gleiche Anfragen teilen sich einen Abruf. Statt `hishel`, weil der fertige `Forecast` gecacht wird, nicht die HTTP-Antwort. Pro Worker; Redis erst bei Bedarf |
| Datenverarbeitung | numpy | pandas wird nicht mehr gebraucht |
| Config | YAML + Pydantic-Modelle, daraus exportiertes JSON-Schema | Fehler mit Datei und Zeile |
| Einheiten | kleine eigene Einheitentabelle (`core/units.py`) | kein `pint`; nur streng monotone Umrechnungen |
| Frontend | **React 19 + TypeScript + Vite** | |
| Server-State | **TanStack Query** | |
| Zeichnen | **SVG mit d3-scale / d3-shape** in React-Komponenten | |
| API-Typen | `openapi-typescript` aus dem eingecheckten OpenAPI-Schema | TypeScript deshalb auf **5.9** gepinnt: openapi-typescript 7 verlangt `typescript ^5` |
| Tests | pytest, Vitest + Testing Library, Playwright | Playwright mit eigenem, gepinntem Chromium |
| Auslieferung | ein Container: gunicorn mit `uvicorn-worker`, FastAPI liefert auch das Frontend aus | `uvicorn.workers` ist veraltet, das Paket `uvicorn-worker` ersetzt es |

## 4. Datenmodell (Nachfolger von `allMeteogramData`)

Das alte Format hatte doppelte Verschachtelung (`['tp']['tp']`), Zeitangaben als
`YYYYMMDD`/`HHMM` plus Stunden-Offsets als Strings und keine Einheiten. Das neue Modell
(`core/model.py`, `Forecast`) macht Einheit, Aggregation und Herkunft explizit:

```jsonc
{
  "location": { "lat": 52.25, "lon": 10.5, "elevation_m": 80, "timezone": "Europe/Berlin",
                "name": "Braunschweig, Deutschland" },          // Gitterzelle, die Open-Meteo verwendet
  "run":      { "source": "open-meteo:ecmwf_ifs025", "init_time": null, "members": 51 },
  "deterministic_run": null,                                     // ab Phase 5
  "steps": ["2026-09-28T22:00:00Z", "2026-09-29T04:00:00Z", …],  // ISO 8601, immer UTC
  "step_hours": 6,
  "variables": {
    "temperature_2m": {
      "unit": "degC", "kind": "instant", "window_hours": null,
      "quantiles": { "p0": […], "p10": […], "p17": […], "p25": […], "p50": […], "p75": […], "p83": […], "p90": […], "p100": […] },
      "deterministic": null
    },
    "precipitation":  { "unit": "mm", "kind": "sum", "window_hours": 6, "quantiles": {…} },
    "cloud_cover":    { "unit": "percent", "kind": "instant", "quantiles": {…} },
    "wind_speed_10m": { "unit": "m/s", "kind": "instant", "quantiles": {…} }
  },
  "pictograms": {
    "precipitation": { "scheme": "precipitation-vsup",
                       "items": [ { "pictogram": "rain/step2_mostly_dry.svg", "level": 2, "class": "none+light" }, … ] },
    …
  }
}
```

`init_time` bleibt leer: Open-Meteo meldet den Startzeitpunkt des Modelllaufs nicht, und der erste
Schritt ist die lokale Mitternacht des Abrufs, nicht der Laufbeginn.

Regeln, wie sie umgesetzt sind:

- **Summen-Variablen werden pro Member aggregiert, bevor Quantile gebildet werden.** Gesteuert
  über `kind: sum` in `core/variables.py`; jede neue Summen-Variable wird damit automatisch richtig
  behandelt.
- **Das Fenster eines Summen-Schritts ist `[t, t+6h)`**, also die Stunden `t+1 … t+6`. Open-Meteo
  liefert pro Stunde die Summe der *vorangehenden* Stunde. Der Altcode summierte `t … t+5` und lag
  damit eine Stunde zu früh; bei Reykjavik verschob das den Median um bis zu 0,9 mm pro Fenster.
- **Horizont:** Der Adapter fragt 15 Tage an. Der Modelllauf endet um Stunde 350, Open-Meteo füllt
  den Rest mit NaN. Die Reduktion schneidet NaN am Ende ab und lehnt Lücken mittendrin ab. Alle
  Variablen teilen sich die Schritte, die eine 6-h-Summe füllen kann: 58–59 statt früher 56.
- Die Quantil-Stufen sind in `vsup.yaml` konfigurierbar, derzeit 0/10/17/25/50/75/83/90/100:
  die drei Bänder der Temperatur (0–100, 10–90, 25–75), der Median und 17–83 für „sicher“. Die
  Regeln dürfen nur Quantile verwenden, die auch berechnet werden; das prüft der Validator.
- **Zeiten sind im Modell immer UTC.** Die Umrechnung in Ortszeit macht das Frontend, in der
  Zeitzone des Vorhersageorts, nicht des Browsers.
- **Das Frontend schneidet den Zeitraum selbst zu.** Die API liefert immer den vollen Horizont.
  Damit ist der alte Fehler behoben, dass Meteogramme fest beim zweiten Schritt begannen statt bei
  „jetzt“.

Der alte Vertrag in `tests/schema.py` bleibt für die im alten Format aufgezeichneten Fixtures; alles
andere prüft das `Forecast`-Modell mit seinen Validatoren.

## 5. Einheiten: Wind korrekt von Anfang an

Kanonische Einheiten im ganzen System:

| Größe | Einheit | Bemerkung |
|---|---|---|
| Temperatur | `degC` | |
| Niederschlag | `mm` pro Fenster (`window_hours`) | Schwellen beziehen sich auf das Fenster; ein Schema für ein anderes Fenster wird beim Start abgelehnt |
| Bewölkung | `percent` (0–100) | |
| Wind | **`m/s`** | Beaufort-Grenzen sind in m/s definiert |

Umgesetzt auf allen drei Ebenen:

1. **An der Quelle anfordern:** `wind_speed_unit=ms` (dazu `temperature_unit`,
   `precipitation_unit`). Seit Phase 0 schon im Altcode.
2. **Adapter prüfen die gemeldete Einheit:** Der Adapter liest die Einheit jeder Reihe aus den
   Flatbuffer-Metadaten und rechnet um oder lehnt ab. Das ist nötig, weil Open-Meteo einen
   unbekannten Parameter stillschweigend ignoriert und dann wieder km/h liefert.
3. **Die Config deklariert ihre Einheit:** Jedes Schema trägt `unit`. Eine andere als die
   kanonische Einheit wird beim Laden einmal umgerechnet, nie zur Laufzeit; eine nicht umrechenbare
   wird abgelehnt.

Die Schwellen 3 / 10 / 17,2 m/s sind unverändert (Parität). Der frühere xfail `TestUnitMismatch`
ist zu regulären Tests geworden: „4,8 m/s wird als leichter Wind klassifiziert.“

## 6. VSUP-Konfiguration

### 6.1 Anforderungen (erfüllt)

- Neue Piktogramme hinzufügen, ohne Python-Code zu ändern.
- Anzahl der Stufen und der Intensitätsklassen frei wählbar.
- Klassengrenzen und die Quantile, die über die Sicherheit entscheiden, frei wählbar.
- Mehrere Schemata nebeneinander; welches ein Produkt nutzt, steht in `sources.yaml`.
- Die alte Logik lässt sich **exakt** abbilden, einschließlich ihrer Eigenheiten.
- Fehler werden beim Laden gefunden, nicht beim ersten Request.

### 6.2 Modi

**Modus `tree`** beschreibt VSUP, wie es gemeint ist: Intensitätsklassen, die mit sinkender
Sicherheit zu Gruppen verschmelzen. Eine Stufe gilt, wenn beide Enden ihres Quantil-Intervalls in
*einer* Gruppe liegen; die Stufen werden von sicher nach unsicher geprüft, die letzte gilt immer.

- **sicher (3):** die mittleren zwei Drittel der Member (p17–p83) in einer Klasse, angelehnt an
  „wahrscheinlich“ (≥ 66 %) in der Sprache des IPCC,
- **wahrscheinlich (2):** die mittlere Hälfte (p25–p75) in einer Gruppe,
- **unsicher (1):** sonst.

So sieht es in `config/vsup.yaml` aus:

```yaml
  precipitation-vsup:
    variable: precipitation
    unit: mm
    window_hours: 6
    mode: tree
    classes:
      - { id: none, below: 0.1 }
      - { id: light, below: 1.0 }
      - { id: medium, below: 2.0 }
      - { id: heavy }
    levels:
      - { level: 3, interval: [p17, p83], groups: [[none], [light], [medium], [heavy]] }
      - { level: 2, interval: [p25, p75], groups: [[none, light], [medium, heavy]] }
      - { level: 1, groups: [[none, light, medium, heavy]] }
    pictograms:
      "3:none": rain/step3_dry.svg
      "3:light": rain/step3_light_rain_v2.svg
      "3:medium": rain/step3_medium_rain.svg
      "3:heavy": rain/step3_heavy_rain.svg
      "2:none+light": rain/step2_mostly_dry.svg
      "2:medium+heavy": rain/step2_mostly_rainy.svg
      "1:none+light+medium+heavy": rain/step1_v2.svg
```

Ebenso `cloud-vsup` (klar < 10 %, leicht < 50 %, bewölkt < 90 %, bedeckt) und `wind-vsup`
(windstill < 3, leicht < 10, stark < 17,2 m/s, Sturm). Diese drei nutzt das Standardprodukt.

**Modus `rules`** ist eine geordnete Regelliste; die erste zutreffende Regel gewinnt, genau wie die
alten `if`-Ketten. Die drei `*-legacy`-Schemata übertragen die alten Funktionen Regel für Regel,
mit den PNGs, die der alte Renderer zeichnete. Die Reihenfolge ist die des Codes, nicht die des
ursprünglichen Plan-Beispiels:

```yaml
  precipitation-legacy:
    variable: precipitation
    unit: mm
    window_hours: 6
    mode: rules
    rules:
      - { when: "p90 < 0.1", pictogram: rain/Stufe3_KeinRegen.png, level: 3, class: none }
      - { when: "p10 > 2", pictogram: rain/Stufe3_Starkregen.png, level: 3, class: heavy }
      - { when: "p10 > 1 and p90 < 2", pictogram: rain/Stufe3_MittlererRegen.png, level: 3, class: medium }
      - { when: "p10 > 1", pictogram: rain/Stufe2_Regen.png, level: 2, class: medium+heavy }
      - { when: "p90 < 1", pictogram: rain/Stufe3_leichterRegen.png, level: 3, class: light }
      - { when: "p50 > 1", pictogram: rain/Stufe2_Regen.png, level: 2, class: medium+heavy }
      - { when: "p75 < 1.5", pictogram: rain/Stufe2_KaumRegen.png, level: 2, class: none+light }
      - { default: true, pictogram: rain/step1_v2.png, level: 1, class: none+light+medium+heavy }
```

Die `when`-Ausdrücke wertet ein **eigener kleiner Parser** aus, nie `eval`. Erlaubt sind
Quantil-Namen, `deterministic`, Zahlen, `< <= > >=`, `and`, `or`, `not`, Klammern und (zusätzlich
zum Plan) verkettete Vergleiche wie `1 < p50 <= 2`. Fehler nennen Zeile und Spalte.

**Modus `anchored`** für HRES-Schemata (Klasse aus dem deterministischen Lauf, Stufe aus der
Übereinstimmung der Member) ist **noch nicht umgesetzt** (Phase 5). Offen ist dabei weiterhin, ob
die alte HRES-Logik bei den mittleren Klassen nur die nächstgelegene Grenze prüft oder beide
(`agreement_bounds: nearest | both`).

### 6.3 Validierung beim Laden

`python -m vsup check` und der Serverstart brechen ab und nennen **alle** Probleme auf einmal, mit
Datei und Zeile, wenn:

- eine referenzierte Piktogrammdatei fehlt oder der Pfad `pictogram_root` verlässt,
- Klassengrenzen nicht streng aufsteigend sind oder die letzte Klasse nicht offen ist,
- eine Regel oder ein Intervall ein Quantil verwendet, das nicht berechnet wird,
- `unit` nicht in die kanonische Einheit der Variable umrechenbar ist, oder bei Summen
  `window_hours` fehlt,
- im `tree`-Modus Gruppen Klassen auslassen, umordnen oder eine Gruppe der Stufe darüber
  aufteilen, eine Kombination aus Stufe und Gruppe kein Piktogramm hat oder ein Piktogramm zu
  keiner Kombination passt,
- im `rules`-Modus die letzte Regel kein `default` ist oder ein `default` nicht am Ende steht,
- ein YAML-Schlüssel doppelt vorkommt (YAML würde stillschweigend nur den letzten behalten).

Die Fähigkeitsprüfung zwischen Schemata und Quellen (Nachfolger von `HresDataUnavailable`) macht der
Loader von `sources.yaml`, siehe 7.2.

`python -m vsup check` zeigt außerdem eine **Abdeckungstabelle**: wie oft jede Regel auf den
Test-Fixtures greift. Nie greifende Regeln fallen so sofort auf.

### 6.4 Piktogramm-Assets

- Die Piktogramme liegen in `webapp/pictograms/`. Die Tree-Schemata verwenden die vorhandenen
  **SVGs**; die Legacy-Schemata die PNGs, die der alte Renderer zeichnete. Deren Regen-Symbole haben
  einen weißen, nicht transparenten Hintergrund, der auf grau hinterlegten Tagen sichtbar wird.
- Die 48 HRES-PNGs (`*/enhanced_hres/`) bleiben als Designvorlage liegen, bis Phase 5 sie als SVG
  neu zeichnet (vermutlich eine SVG pro Klasse, Farbe per CSS).
- **Farbe pro Stufe in der Config** (`level_styles`) ist noch nicht umgesetzt; die Farben stecken
  weiterhin in den Grafikdateien.
- Die **Legende erzeugt das Frontend aus dem Schema** (`/api/schemes`), inklusive der Klassengrenzen
  und dessen, was jede Stufe verlangt („mind. 66 % der Mitglieder in einer Klasse“), abgeleitet aus
  den Stufen-Intervallen. Die alten PNG-Legenden (`vsup_all.png` u. a.) sind entfernt.

## 7. Datenquellen

### 7.1 Adapter-Schnittstelle

```python
class ForecastSource(Protocol):
    id: str                                  # "open-meteo:ecmwf_ifs025"
    kind: Literal["ensemble", "quantiles"]   # "deterministic" kommt mit Phase 5
    variables: frozenset[str]                # was die Quelle liefern kann
    quantile_levels: tuple[int, ...] | None  # Quantil-Quellen: welche Stufen es gibt
    max_lead: timedelta
    native_step: timedelta

    async def fetch(self, location: Location, variables: set[str]) -> SourceResult: ...
```

`SourceResult` enthält entweder Member-Arrays (`ensemble`) oder fertige Quantile (`quantiles`),
immer in kanonischen Einheiten und UTC. Die gemeinsame Pipeline übernimmt die Reduktion auf das
6-h-Raster (Summen summiert, Momentanwerte abgetastet) und Member → Quantile; die Klassifikation
folgt pro Produkt. Die Angleichung von Ensemble und HRES auf ein gemeinsames Raster kommt mit
Phase 5.

### 7.2 Quellen-Konfiguration

So sieht `config/sources.yaml` heute aus:

```yaml
version: 1

sources:
  ecmwf-ens:
    adapter: open_meteo_ensemble
    model: ecmwf_ifs025
    forecast_days: 15
    timeout_s: 30

products:
  ecmwf:
    label: ECMWF ensemble
    ensemble: ecmwf-ens
    schemes: [cloud-vsup, precipitation-vsup, wind-vsup]
```

`config/sources.fixtures.yaml` ist das Offline-Gegenstück mit demselben Produktnamen, aus den
aufgezeichneten Vorhersagen (`SOURCES_CONFIG=config/sources.fixtures.yaml`).

Beim Laden wird jedes Produkt geprüft: Jedes Schema existiert, zeichnet eine Variable, die die
Quelle liefert, ist für das 6-h-Fenster geschrieben, liest keinen deterministischen Lauf, und eine
Quantil-Quelle bietet alle Quantile, die `vsup.yaml` berechnet. Zwei Schemata für dieselbe
Variable werden abgelehnt. Was sonst bei einer Anfrage scheitern würde, scheitert beim Start.

Ein Produkt ohne deterministische Quelle bietet die HRES-Variante gar nicht erst an; die API
antwortet auf `variant=hres` mit 409, die Oberfläche zeigt die Auswahl nicht. Die Felder
`deterministic` und `hres_schemes` pro Produkt kommen mit Phase 5.

### 7.3 Adapter

| Adapter | Zweck | Stand |
|---|---|---|
| `open_meteo_ensemble` | ECMWF IFS 0,25° (51 Member) | umgesetzt. Flatbuffers statt JSON, weil JSON auf eine Nachkommastelle rundet |
| `fixture` | liest `tests/fixtures/*.json` | umgesetzt. Liest das alte `allMeteogramData`-Format und das neue `Forecast`-Format |
| `open_meteo_forecast` | deterministische Läufe (`ecmwf_ifs` …) | Phase 5. Vorher klären, welches Produkt `ecmwf_ifs` bei Open-Meteo genau ist (Auflösung, Initialisierungszeit) |
| weiteres Ensemble, z. B. DWD ICON-EPS | zweites Produkt | Phase 5, nur über `sources.yaml` und den vorhandenen Adapter |
| später: `ecmwf_opendata` | direkt ECMWF Open Data (GRIB) | nur falls Open-Meteo nicht reicht |

**Geocoding über Open-Meteo statt Nominatim** (Abweichung vom Plan): Die Ortssuche sucht während
des Tippens. Die Nutzungsbedingungen von Nominatim verbieten das ausdrücklich („you must not
implement such a service on the client side using the API“) und erlauben höchstens eine Anfrage
pro Sekunde. Die Geocoding-API von Open-Meteo ist dafür gebaut, kommt vom selben Anbieter und
liefert Zeitzone und Höhe gleich mit. Sie steckt hinter einer eigenen Schnittstelle
(`sources/geocode.py`) und ist austauschbar. `timezonefinder` wird nicht mehr gebraucht:
Open-Meteo meldet die Zeitzone mit jeder Vorhersage.

## 8. API

| Methode & Pfad | Antwort |
|---|---|
| `GET /api/forecast?lat=&lon=&product=&variant=&name=` | Datenmodell aus Abschnitt 4 über den vollen Horizont, mit den Piktogrammen des Produkts. Koordinaten werden auf 2 Nachkommastellen gerundet (≈ 1 km, das Modellgitter hat 25 km) |
| `GET /api/geocode?q=&lang=de\|en&count=` | Trefferliste `{name, lat, lon, admin1, country, country_code, timezone, elevation_m}`; leer statt Fehler, wenn nichts gefunden wird |
| `GET /api/products` | verfügbare Produkte und Varianten, abgeleitet aus `sources.yaml` |
| `GET /api/schemes` | alle geladenen VSUP-Schemata mit allen möglichen Piktogrammen, Klassengrenzen und Stufen-Intervallen (für die Legende), Version und Piktogramm-Basis-URL |
| `GET /api/health` | Liveness; `?deep=true` fragt zusätzlich jede Quelle ab (503, wenn eine ausfällt) |
| `GET /pictograms/<version>/…` | statische Piktogramme; die Version ändert sich mit Config oder Datei, daher `immutable` cachebar |
| `GET /` | das gebaute Frontend (`index.html` ohne Cache, gehashte Assets dauerhaft cachebar) |

Fehlerverhalten, alle mit `{"detail": …}`:

- ungültige Parameter → 422 mit Feldangabe, auch ein unbekanntes Produkt
- keine Daten für den Ort (etwa keine Fixture in der Nähe) → 404
- Variante für dieses Produkt nicht verfügbar → 409
- Upstream-Fehler oder unbrauchbare Daten → 502, Upstream-Timeout → 504
- andere Methoden als GET → 405

Die API ist rein lesend. Es gibt weder CSRF-Schutz noch `SECRET_KEY`, weil es keine Sessions gibt.
Ein Test schlägt fehl, sobald eine Route etwas anderes als GET annimmt.

## 9. Frontend

### Aufbau

```
webapp/frontend/src/
  api/          client.ts (typisierter Zugriff), queries.ts (TanStack Query),
                schema.d.ts (generiert aus api/openapi.json)
  state/        urlState.ts: URL-Parameter als einzige Quelle für Ort/Produkt/Variante/Tage
  meteogram/
    Meteogram.tsx   Wahl der Darstellung, Tastatur, gemeinsamer Zustand
    RowChart.tsx    Zeile: Zeitachse, Piktogramm-Reihen, Quantilband, Tagesextreme,
                    Tagesschattierung, Crosshair; scrollt seitlich, wenn nötig
    ScrollIndicator.tsx  Tageskarte unter der scrollenden Zeile mit Rahmen um den sichtbaren Teil
    ColumnChart.tsx dasselbe senkrecht: Zeit nach unten, Spalten statt Reihen
    Tooltip.tsx     alle Quantile, Klasse und Stufe des gewählten Schritts
    layout.ts       reine Logik: sichtbarer Bereich, Tage in Ortszeit, Extreme
    time.ts         Ortszeit in beliebiger Zeitzone über Intl, inkl. Sommerzeit
  legend/       Legende aus /api/schemes
  search/       Ortssuche mit Debounce, Koordinaten-Eingabe, "Mein Standort"
  i18n.ts       Deutsch und Englisch
```

### Verhalten

- **Der Zustand steckt in der URL** (`?lat=52.26&lon=10.52&name=Braunschweig&days=7`). Links sind
  teilbar, Zurück/Vor funktionieren. Koordinaten werden beim Zurückschreiben nie gerundet, sonst
  verschöbe schon die erste Änderung am Tage-Regler den Ort.
- **Zeitraum ohne neuen Server-Aufruf:** Die API liefert den vollen Horizont; der Tage-Regler
  schneidet nur zu. Die Ansicht beginnt beim Schritt, der „jetzt“ am nächsten liegt; eine
  veraltete Vorhersage wird ab ihrem Anfang mit Hinweis gezeigt.
- **Alle Piktogramme am Zeitpunkt ihres Schritts:** Wolken, Wind und Temperatur bei `t`, die
  Niederschlagssumme über `[t, t+6 h)` ebenfalls bei `t`, in einer Spalte mit den übrigen. Bis
  01.10.2026 stand sie in der Mitte ihres Fensters (`t + 3 h`); zwischen den Zeitpunkten wirkte die
  Regenreihe aber verrutscht. Das Fenster zeigt weiter das Fadenkreuz.
- **Schmale Bildschirme: zwei umschaltbare Darstellungen** statt 12-h-Zusammenfassung (Abweichung
  vom Plan, nach Test auf dem Handy; Wahl in der URL als `layout=vertical`).
  **Waagerecht:** alles in einer Zeile. Würde ein Zeitschritt schmaler als 23 px, scrollt die Zeile
  seitlich (Wischen, Trackpad), darunter eine Tageskarte mit Rahmen um den sichtbaren Teil, zum
  Ziehen und mit ‹ ›-Knöpfen; die Temperaturbeschriftung bleibt stehen. **Senkrecht:** die Zeit
  läuft nach unten, die Größen stehen als Spalten nebeneinander, die Spaltenköpfe bleiben oben.
  Die frühere Lösung, lange Meteogramme in Abschnitte untereinander zu teilen, wirkte zerstückelt
  und ist entfallen (30.09.2026). Der `step_hours`-Parameter entfällt damit vorerst.
- **Export** als SVG/PNG: noch nicht umgesetzt.
- **Barrierefreiheit:** Jedes Piktogramm hat einen `<title>` („kein Regen oder leichter Regen
  (wahrscheinlich)“); das Meteogramm ist per Tastatur bedienbar (Pfeiltasten, Pos1/Ende), der
  Tooltip ist eine Live-Region für Bildschirmleser.
- **i18n:** Deutsch und Englisch, per `?lang=` oder Browsersprache; Wochentage und Uhrzeiten über
  `Intl` in der Zeitzone des Orts.
- „Mein Standort“ funktioniert nur über HTTPS (oder `localhost`); im Heimnetz über HTTP nicht.
- Nur helles Design; ein Dark Mode fehlt.

## 10. Migrationsplan

Jede Phase endete mit etwas Lauffähigem.

### Phase 0 – Wind im Bestand korrigieren · erledigt (`dc7984f`)
- Der Altcode fragte `wind_speed_unit=ms` an und prüfte die gemeldete Einheit jeder Variable.
- `TestUnitMismatch` wurde zu regulären Tests; Fixtures neu erzeugt.

### Phase 1 – Kern extrahieren · erledigt (`045b77f`, `dbad5fe`)
- `core/`, `sources/`, `vsup/` mit Datenmodell, Einheitenprüfung, Reduktion, Adaptern
  `open_meteo_ensemble` und `fixture`, VSUP-Loader mit `rules` und `tree`, Validator, CLI.
- **Golden-Tests:** Die Legacy-Schemata liefern auf allen fünf Fixtures und auf einem vollständigen
  Gitter um jede Schwelle exakt dieselben Piktogramme wie die alten Funktionen. Gegenprobe per
  Mutation (`<=` statt `<`, verschobene Schwelle, vertauschte Regeln).

### Phase 2 – API · erledigt (`cb10270`, `6f15f5f`)
- FastAPI mit den Endpunkten aus Abschnitt 8, `sources.yaml` mit Fähigkeitsprüfung beim Start,
  Geocoder, Cache, Live-Tests, eingechecktes OpenAPI-Schema als Vertrag.

### Phase 3 – Frontend-MVP · erledigt (`765b45a`, `d0503b4`)
- Ortssuche, Meteogramm mit vier Reihen, Legende aus der Config, URL-Zustand, Hover und Tastatur.
- Playwright-Screenshots der fünf Fixture-Orte und auf dem Handy (Adapter `fixture`, eingefrorene
  Uhr, daher deterministisch); danach die Abschnitte für lange Zeiträume, am 30.09.2026 ersetzt
  durch eine seitlich scrollende Zeile und eine senkrechte Darstellung.

### Phase 4 – Umstellung · erledigt (`862f4c7`, `e1bf118`, `497e098`, `af90976`)
- Ein Container liefert Frontend und API aus; Frontend in einer Node-Build-Stufe, gunicorn mit
  `uvicorn-worker`, kein Compiler im Image, Healthcheck.
- Sichtprüfung neu gegen alt auf allen fünf Fixtures: mit den Legacy-Regeln Symbol für Symbol
  gleich, gleiche Tagesextreme.
- Entfernt: Flask, flask_wtf, flask_bootstrap, matplotlib und die übrigen Altpakete, `app/`,
  `run.py`, `config.py`, `downloadJsonData.py`, `plotMeteogram.py` (damit auch der tote
  15-Tage-Zweig), `allmeteogramdata.json`, die PNG-Legenden, die Root-`requirements.txt` und
  `plotly.html`. Die Piktogramme sind nach `webapp/pictograms/` gezogen.
- Behalten, nach Entscheidung: die alten Regel-Funktionen wortgleich als Testreferenz
  (`tests/legacy_reference.py`); die HRES-PNGs. Die alten Fixtures blieben zunächst unverändert;
  mit der Umstellung von „sicher“ auf p17–p83 (die das alte Format nicht enthält) wurden sie am
  29.09.2026 im `Forecast`-Format neu aufgenommen. Eine alte Datei bleibt als Muster in
  `tests/fixtures/legacy/`, damit der Leser für das alte Format getestet bleibt.
- Beim ersten Container-Start zeigte sich: `COPY` übernimmt die Dateirechte des Checkouts, und
  `startup.sh` und die Piktogramme waren für den unprivilegierten Benutzer nicht lesbar. Seit
  `497e098` normalisiert das Dockerfile die Rechte.

### Phase 5 – HRES und weitere Quellen · teilweise

Der zweite Punkt ist erledigt und weit darüber hinaus: `docs/modelle-plan.md` hat am 01.10.2026 acht
weitere Ensembles eingebaut (ICON global, ICON-EU, ICON-D2, MeteoSwiss ICON-CH2, AIFS, GFS, AI-GEFS,
GEM), eine automatische Wahl des feinsten passenden Modells und stündliche Schritte für ICON-D2 und
ICON-CH2. Jede neue Quelle brauchte nur `sources.yaml`; Codeänderungen betrafen Gebiete, Schrittweiten
und die Auswahl, nicht die Adapter. Offen ist HRES:

- Adapter `open_meteo_forecast`, Angleichung der Zeitachsen Ensemble/HRES, HRES-Schemata im Modus
  `anchored`, `deterministic`/`hres_schemes` pro Produkt, 48 HRES-Piktogramme als SVG.
- Zweites Produkt (z. B. DWD ICON-EPS) nur über `sources.yaml`, als Beleg, dass eine neue Quelle
  ohne Codeänderung außerhalb eines Adapters möglich ist.

### Phase 6 – Ausbau (optional) · offen
- Config-Playground: Schema im Browser bearbeiten und live auf die aktuelle Vorhersage anwenden.
  Dafür bräuchte es eine zweite Implementierung des Klassifikators in TypeScript (gegen dieselben
  Golden-Fixtures getestet) oder den Klassifikator als WebAssembly (Pyodide).
- Weitere Variablen: Schnee, Böen, Gewitterwahrscheinlichkeit (CAPE), jeweils nur Config +
  Piktogramme, solange die Quelle sie liefert.
- Mehrere Orte nebeneinander vergleichen.

## 11. Teststrategie

| Ebene | Was | Werkzeug |
|---|---|---|
| Einheiten | Adapter liefern kanonische Einheiten; eine Config mit falscher Einheit wird abgelehnt; 4,8 m/s ist „leichter Wind“ | pytest |
| Aggregation | Summen pro Member vor Quantilen; Fenster `t+1 … t+6`, geprüft gegen die Roh-Member einer aufgezeichneten Antwort | pytest |
| VSUP | Golden-Tests Legacy-Parität; Grenzwerte jeder Schwelle; ein Fall pro Validator-Fehler mit Zeilennummer | pytest |
| API | Statuscodes, Schema, nur GET, Cache, Frontend-Auslieferung, Timeouts | pytest + `httpx.ASGITransport` / `MockTransport` |
| Container | Einstiegspunkt, Build-Stufen, kopierte Pfade, Dateirechte, Healthcheck | pytest (statisch) + einmal real gebaut und geprüft |
| Live | Open-Meteo-Format und -Einheiten, Adapter, Geocoder, API durchgängig | `pytest -m live` |
| Frontend | Layout, Zeitzonen inkl. 25-h-Tag, URL-Zustand, Legende, Suche, Meteogramm | Vitest + Testing Library |
| E2E | Suche → Meteogramm, URL-Zustand, Screenshots der fünf Fixtures und auf dem Handy | Playwright |

Stand: 336 pytest-Tests (davon 8 live), 80 Vitest-, 14 Playwright-Tests; die Python-Suite läuft
auch in einer frischen venv nur mit `requirements.txt`.

Die bewährten Konventionen bleiben: Datums-Werte in Tests kommen aus der Fixture selbst und nie aus
`now()`, bekannte Fehler werden als `strict=True`-xfail festgehalten, und es gibt genau eine
Schema-Definition pro Format.

**Noch offen: eine CI-Pipeline.** Es gibt kein Remote; eine CI setzt eine Plattform voraus (etwa
GitHub Actions). Mindestens: `pytest -m "not live"`, `vsup check`, `sources check`, `tsc`,
`npm run check:api`, Vitest und Playwright bei jedem Push.

## 12. Risiken und offene Entscheidungen

| Punkt | Stand | Offen für |
|---|---|---|
| Klassifikation im Backend oder im Browser | entschieden: Backend | Umkehr nur, falls der Config-Playground Priorität bekommt |
| Config-Format | entschieden: YAML + JSON-Schema | – |
| Ausdruckssprache in `rules` | entschieden: eigener Mini-Parser | – |
| Was „sicher“ bei den Tree-Schemata heißt | entschieden (29.09.2026): mind. zwei Drittel der Member in einer Klasse (p17–p83) | Verglichen auf Live-Daten der fünf Orte: 80 % machte nur die Randklassen je „sicher“ (Wolken 9 %, Regen 18 % der Schritte), 50 % war als Aussage „sicher“ zu schwach, 66 % ergibt 13 % / 26 % / Wind 60 %. Mittlere Klassen (leichter Regen, leicht bewölkt) sind weiter fast nie sicher; das liegt an ihrer schmalen Spanne, nicht an der Schwelle – ändern ließe sich das nur über die Klassengrenzen |
| HRES-Quelle | offen | Open-Meteo `ecmwf_ifs`; Produktdetails prüfen |
| Schwellen 3 / 10 m/s oder Beaufort-exakt 3,4 / 10,8 m/s | unverändert (Parität) | fachliche Entscheidung, per Config änderbar |
| Harte Klassengrenzen führen zu Symbolsprüngen | vorerst beibehalten | eventuell Hysterese oder Unschärfebereich im `tree`-Modus |
| Open-Meteo-Rate-Limits bei öffentlichem Betrieb | Cache 1 h, gleiche Anfragen gebündelt | kein eigenes Rate-Limiting; kommerzieller API-Key oder gemeinsamer Cache (Redis), falls die Nutzung wächst |
| SVG-Neuzeichnung der HRES-Piktogramme | offen | Designarbeit, nicht Code |
| Merge nach `main` | erledigt (29.09.2026), alte App auf `old-webapp` | – |

## 13. Aufwand

Die Phasen 0–4 sind umgesetzt. Die ursprüngliche Schätzung für das, was offen ist, bleibt:

| Phase | Aufwand |
|---|---|
| 5 HRES + zweite Quelle | 4–6 Tage (+ Designzeit für SVGs) |
| 6 Ausbau | nach Umfang |
| CI-Pipeline | ca. 0,5 Tage, sobald eine Plattform feststeht |
