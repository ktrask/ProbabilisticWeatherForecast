# Plan: weitere Ensemble-Modelle und stündliche Vorhersage

Stand: 01.10.2026 · Stufen 1 bis 3 umgesetzt, Stufen 4 und 5 offen. Baut auf dem Stand von `main` auf (Commit
`83bd6bd`) und ergänzt Phase 5 aus `neuentwicklung-plan.md` („Zweites Produkt nur über `sources.yaml`“).

| Stufe | Stand |
|---|---|
| 1 Zweites globales Modell | erledigt (01.10.2026) |
| 2 Regionale Modelle | erledigt (01.10.2026) |
| 3 Automatische Vorauswahl | erledigt (01.10.2026) |
| 4 Stündliche Schritte | offen |
| 5 Weitere globale Modelle | offen |

## 1. Ziel

- Neben dem ECMWF-Ensemble weitere Ensembles von Open-Meteo anbieten, **wählbar** im Frontend.
- **Automatische Vorauswahl:** ECMWF bleibt der Standard für längere Zeiträume. Deckt ein höher
  aufgelöstes Modell den Ort und den gewählten Zeitraum ab, ist es der Standard.
- **Stündliche Schritte** als Option für Modelle, die wirklich stündlich rechnen.
- **Schrittweise:** Jede Stufe unten ist für sich lauffähig und testbar, im Browser und auf dem Handy,
  bevor die nächste beginnt. Jede Stufe ist ein eigener Commit (oder Branch, nach Wunsch).

## 2. Was Open-Meteo anbietet (geprüft am 30.09.2026)

Alle Modelle unten liefern die vier Größen der App (Temperatur, Niederschlag, Bewölkung, Wind in m/s).
Abgefragt für Döteberg bei Hannover, die MeteoSwiss-Modelle für Zermatt.

| `models=` | Anbieter | Member | Gitter | Reichweite | intern | Gebiet |
|---|---|---|---|---|---|---|
| `ecmwf_ifs025` | ECMWF | 51 | 25 km | ~14 Tage | 3-/6-stündlich | global – **heute in der App** |
| `ecmwf_aifs025` | ECMWF (KI) | 51 | 25 km | 15 Tage | 6-stündlich | global |
| `icon_seamless_eps` | DWD | 40 | 13–26 km | 7,5 Tage | stündlich | global |
| `icon_eu_eps` | DWD | 40 | 13 km | 5 Tage | stündlich | Europa |
| `icon_d2_eps` | DWD | 20 | 2 km | 2 Tage | stündlich | Mitteleuropa |
| `meteoswiss_icon_ch2` | MeteoSwiss | 21 | 2 km | 5 Tage | stündlich | Alpenraum |
| `meteoswiss_icon_ch1` | MeteoSwiss | 11 | 1 km | 1,5 Tage | stündlich | Alpenraum |
| `gfs_seamless` | NOAA | 31 | 25–50 km | 16 Tage | 3-stündlich | global |
| `ncep_aigefs025` | NOAA (KI) | 31 | 25 km | 16 Tage | 6-stündlich | global |
| `gem_global` | Kanada | 21 | 25 km | 16 Tage | 3-stündlich | global |
| `ukmo_global_ensemble_20km` | UK Met Office | 18 | 20 km | 10 Tage | stündlich | global |
| `ukmo_uk_ensemble_2km` | UK Met Office | 3 | 2 km | 5 Tage | stündlich | UK und Umgebung |

Ungeklärt: Die Doku nennt außerdem **ECMWF IFS Europe 9 km** (51 Member, die ersten 90 h stündlich),
AIFS Europe und Google WeatherNext 2. Für das IFS 9 km ist `ecmwf_ifs` wahrscheinlich der Name: Die API
nimmt ihn an und antwortet mit einem Punkt des feinen Gitters (52,408° / 9,518° für Döteberg), am
01.10.2026 aber ohne Werte und mit nur einem „Member“. Später erneut prüfen. Es wäre für Europa der
natürliche Nachfolger des heutigen Standards.

**Abdeckung:** Außerhalb seines Gebiets antwortet ein Regionalmodell mit „No data is available for
this location“ (ICON-D2-EPS für Rom oder Singapur, MeteoSwiss für Braunschweig). Ob ein Modell einen
Ort abdeckt, lässt sich also verlässlich erfragen. Die Gebiete sind gedrehte oder projizierte Gitter;
ein Rechteck in der Konfiguration ist deshalb nur ein Vorfilter, die Antwort entscheidet.

## 3. Was im Code heute auf ein Modell und 6 Stunden festgelegt ist

- `core/pipeline.py`: `STEP_HOURS = 6`. `build_forecast(step_hours=…)` kann schon andere Schritte.
- `sources/config.py`: Die Startprüfung lehnt Schemata für andere Fenster als 6 h ab und Quellen
  mit anderem nativen Schritt.
- `config/vsup.yaml`: Das Niederschlagsschema ist für 6-h-Summen geschrieben (0,1 / 1 / 2 mm).
  Bewölkung, Wind und Temperatur sind Momentanwerte und hängen nicht am Schritt.
- API: `product` wählt ein Produkt, ohne Angabe gilt das erste. Fehler einer Quelle werden zu 502.
  „Kein Modell für diesen Ort“ gibt es noch nicht.
- Frontend: Die Produktauswahl erscheint schon, sobald es mehr als ein Produkt gibt, und das Produkt
  steht bereits in der URL. Die Zeitachse ist auf 6-h-Schritte eingestellt (Stundenbeschriftung).
- Fixtures: aufgezeichnet nur vom ECMWF-Ensemble. `sources.fixtures.yaml` ist der Offline-Zwilling
  mit denselben Produktnamen.

## 4. Stufen

### Stufe 1 – Ein zweites globales Modell, von Hand wählbar · erledigt

Kleinster Schritt, der die Auswahl sichtbar macht.

**Umgesetzt (01.10.2026):**
- Modell ist `icon_global_eps` (26 km überall), **nicht** `icon_seamless_eps`. Letzteres wechselt in
  Europa unbemerkt auf das feinere EU-Gitter, und das verwischte die Regel „feinstes Gitter“ aus
  Stufe 3. ICON-EU kommt in Stufe 2 als eigenes Produkt.
- Jedes Produkt nennt `members`, `grid_km` und `horizon_days`, Pflicht beim Laden. Ein Live-Test
  prüft Member und Reichweite gegen einen echten Lauf. Fehlt ein Feld, nennt die Fehlermeldung jetzt
  den Feldnamen.
- Fixtures für ICON in `tests/fixtures/icon_eps/` (`generate_fixtures.py --model icon_global_eps`).
- Die Auswahl im Frontend zeigt „DWD ICON ensemble (40 Mitglieder, 26 km, bis 7,5 Tage)“. Der
  Tage-Regler folgt der Reichweite des Modells, ohne eigenen Code.
- Nebenbefund: ECMWF reicht je nach neuestem Lauf nur 13 bis 14,5 Tage (die 06/18-UTC-Läufe gehen
  6 Tage weit). Der alte Live-Test (mehr als 14 Tage) war deshalb tageszeitabhängig und ist angepasst.

**Erster Vergleich** (Live-Läufe vom 01.10.2026, je 7 Tage, fünf Orte, Anteil der Schritte
sicher / wahrscheinlich / unsicher):

| | ECMWF (51) | ICON (40) |
|---|---|---|
| Bewölkung | 14 / 48 / 39 % | 21 / 36 / 43 % |
| Niederschlag | 39 / 41 / 21 % | 68 / 21 / 11 % |
| Wind | 71 / 29 / 0 % | 71 / 29 / 0 % |

Der Unterschied beim Niederschlag kommt nicht von der Member-Zahl. ICON ist an diesem Tag viel
trockener: Singapur 3,5 mm Median über die Woche gegenüber 14 mm bei ECMWF, das 90. Perzentil 10
gegenüber 45 mm. Sein „sicher“ ist also fast immer „sicher kein Regen“. Das ist eine Eigenschaft
des Modells (grobes Gitter, parametrisierte Konvektion) und genau das, was die Auswahl sichtbar
machen soll. Sie liest sich aber nicht wie ein Fehler der App, sondern wie ein Modellunterschied.

- `sources.yaml`: Quelle `icon-eps` (`icon_seamless_eps`) und Produkt `icon` mit denselben Schemata.
  Der Adapter bleibt unverändert, `model` ist schon ein Parameter.
- Jedes Produkt bekommt beschreibende Angaben für die Auswahl: Anbieter, Member, Gitterweite, Reichweite.
  `/api/products` liefert sie mit.
- Frontend: Die vorhandene Auswahl zeigt „ECMWF (51 Member, 25 km, 15 Tage)“ usw. Der Tage-Regler
  endet bei der Reichweite des Produkts.
- `python -m vsup check` wertet die Stufenverteilung für jedes Produkt aus. Wichtig, weil die Schwelle
  „zwei Drittel“ an 51 Membern festgelegt wurde.
- Tests: Fixtures für ICON-EPS aufzeichnen (fünf Orte, eigenes Unterverzeichnis), Produkt im
  Offline-Zwilling, API- und Playwright-Test für die Auswahl.
- Den API-Namen von IFS Europe 9 km klären.

**Zum Testen:** Zwischen ECMWF und ICON umschalten, für eigene Orte. Wie verteilen sich die Stufen bei
40 statt 51 Membern?

### Stufe 2 – Regionale Modelle mit Abdeckung, von Hand wählbar · erledigt

**Umgesetzt (01.10.2026):** Produkte `icon-eu` (40 Member, 13 km, 5 Tage), `icon-d2` (20 Member, 2 km,
2 Tage) und `meteoswiss` (ICON-CH2, 21 Member, 2 km, 4,5 Tage). Abweichungen und Befunde:

- Die Gebiete stehen an der **Quelle** (`area`), nicht am Produkt. Sie stammen aus Open-Meteos
  Metadaten (`/data/<domain>/static/meta.json`, `BBOX`). ICON-EU und -D2 füllen ihr Rechteck. Das
  gedrehte MeteoSwiss-Gitter tut es nicht: Bei 42,7° N / 16,7° O liegt der Punkt im Rechteck, die
  Antwort ist aber „No data“.
- Statt `/api/products?lat=…&lon=…` liefert `/api/products` das Rechteck mit, und das Frontend filtert
  selbst. So bleibt die Antwort für alle Orte gleich und cachebar.
- Hat das gewählte Modell für den Ort keine Vorhersage (Rand eines gedrehten Gitters, oder ein Link
  mit fremdem Ort), bietet die Fehlermeldung „Standardmodell verwenden“ an. Ein neuer Ort außerhalb
  des Gebiets wechselt gleich zum Standard.
- Der Health-Check fragt jede Quelle in ihrem Gebiet: am Prüfort, sonst in der Mitte des Rechtecks.
  Die Fixture-Quelle fragt die erste Aufzeichnung.
- Mindestens 10 Member, geprüft beim Laden. Das UK-Modell (3) ist damit draußen.

**Vergleich der ersten zwei Tage** (Live-Läufe 01.10.2026; sicher/wahrscheinlich/unsicher in 8
Schritten; Temperaturband p10–p90 im Mittel):

| | Bewölkung | Niederschlag | Wind | T-Band |
|---|---|---|---|---|
| Braunschweig ECMWF | 5/2/1 | 5/0/3 | 5/3/0 | 2,1 K |
| Braunschweig ICON-EU | 5/2/1 | 5/1/2 | 7/1/0 | 1,3 K |
| Braunschweig ICON-D2 | 5/2/1 | 5/1/2 | 8/0/0 | 1,1 K |
| Zermatt ECMWF | 3/5/0 | 0/6/2 | 8/0/0 | 1,9 K |
| Zermatt ICON-D2 | 4/3/1 | 5/2/1 | 8/0/0 | 1,1 K |
| Zermatt MeteoSwiss | 5/1/2 | 5/3/0 | 8/0/0 | 1,0 K |

Die feinen Modelle sind sich enger einig (halb so breites Temperaturband). In den Alpen wird der
Regen erst mit ihnen überhaupt einmal „sicher“. Ob das Einigkeit ist oder nur geringere Streuung der
kleineren Ensembles, zeigt erst ein Vergleich mit dem, was dann tatsächlich eintritt.

**Ursprünglicher Plan:**

- Quellen und Produkte für `icon_eu_eps`, `icon_d2_eps` und `meteoswiss_icon_ch2`.
- **Abdeckung je Produkt:** ein grobes Rechteck in `sources.yaml` als Vorfilter, dazu die Antwort von
  Open-Meteo. Der Adapter übersetzt „No data is available for this location“ in eine eigene
  Ausnahme `NotCovered` statt in einen 502.
- API: `/api/products?lat=…&lon=…` sagt je Produkt, ob es den Ort abdeckt (Vorfilter). `/api/forecast`
  antwortet bei fehlender Abdeckung mit 404 „Dieses Modell deckt den Ort nicht ab“.
- Frontend: Die Auswahl zeigt nur Produkte, die den Ort abdecken.
- **Mindestzahl an Membern:** Das UK-Modell mit 3 Membern bleibt draußen. Mit 3 Werten sagen p17 und
  p83 nichts. Vorschlag: mindestens 10 Member, beim Laden geprüft.
- Tests: Fixtures je Modell, darunter ein Ort außerhalb, um die Abdeckung offline zu prüfen.

**Zum Testen:** Braunschweig mit ICON-D2-EPS (2 Tage, 2 km), Zermatt mit MeteoSwiss, Singapur bietet
nur die globalen Modelle an.

### Stufe 3 – Automatische Vorauswahl · erledigt

**Umgesetzt (01.10.2026), nach zwei Entscheidungen:** Das Modell wechselt mit dem Tage-Regler (mit
Hinweis), und ICON-D2 wird erst mit stündlichen Schritten automatisch gewählt (`automatic: false`
in `sources.yaml`, von Hand weiter wählbar).

- Die Regel steht in `sources/choice.py` und liefert eine Rangliste, nicht nur ein Produkt. Hat das
  beste am Rand eines gedrehten Gitters doch keine Daten (`NotCovered`), nimmt die API das nächste.
- `/api/forecast` ohne `product` nimmt `days` und sagt in der Antwort `product` und `automatic`.
  Ohne `days` muss ein Produkt so weit reichen wie der Standard, das bleibt also ECMWF, wie bisher.
- Das Frontend wendet dieselbe Regel an (gleiche Testfälle), nur um den Abruf zu schlüsseln: Ein
  Verschieben der Tage lädt nur neu, wenn sich das Modell ändert. Bis dahin bleibt das alte Bild,
  abgeblendet, stehen.
- Auswahlfeld: erster Eintrag „Automatisch (DWD ICON-EU ensemble)“, unter dem Meteogramm
  „…, automatisch gewählt“. Im Automatikmodus bietet der Tage-Regler so viele Tage wie der Standard.
- Ergebnis: Braunschweig bis 5 Tage ICON-EU, danach ECMWF. Zermatt bis 5 Tage MeteoSwiss.
  Reykjavík bis 5 Tage ICON-EU. Singapur ECMWF.

**Ursprünglicher Plan:**

- Ohne `product` in der URL wählt die App selbst: unter den Produkten, die den **Ort abdecken** und den
  **gewählten Zeitraum** schaffen, das mit dem **feinsten Gitter**. Bei Gleichstand gewinnt die
  Reihenfolge in `sources.yaml`. Gibt es keines, gilt ECMWF.
  Beispiel Braunschweig: 1–2 Tage → ICON-D2-EPS, 3–5 Tage → ICON-EU-EPS, ab 6 Tagen → ECMWF.
- Die Wahl trifft das Backend. `/api/forecast` ohne `product` nennt in der Antwort, welches Produkt es
  genommen hat und warum („feinstes Modell für diesen Ort und 2 Tage“). So entscheidet überall dieselbe Regel.
- Frontend: Die Auswahl hat den Eintrag „Automatisch (ICON-D2-EPS)“. Eine Wahl von Hand steht in der URL
  und gilt, bis man wieder „Automatisch“ wählt.
- **Achtung beim Tage-Regler:** Im Automatikmodus wechselt mit der Tageszahl womöglich das Modell, also ein neuer
  Abruf, und das Bild ändert sich sprunghaft. Das muss sichtbar sein (Hinweis am Modellnamen).
- Tests: die Auswahlregel als reine Funktion mit Tabellen-Tests, API-Tests für die Antwort, Playwright
  für das Umschalten per Tage-Regler.

**Zum Testen:** Fühlt sich der Modellwechsel beim Verschieben der Tage richtig an, oder lieber ein
fester Standard pro Ort?

### Stufe 4 – Stündliche Schritte

Nur für Modelle, die intern wirklich stündlich rechnen (ICON-D2/EU-EPS, MeteoSwiss, UKMO).
Bei 3- oder 6-stündlichen Modellen füllt Open-Meteo die Stunden dazwischen auf. Ein stündliches
Meteogramm täuschte dort eine Auflösung vor, die es nicht gibt.

- `sources.yaml`: Jede Quelle nennt ihren internen Schritt (`native_step_hours`), jedes Produkt
  die angebotenen Schritte (`steps: [6, 1]`).
- Pipeline: Die Schrittweite kommt aus der Anfrage statt aus `STEP_HOURS`. Sie ist Teil des
  Cache-Schlüssels.
- `vsup.yaml`: ein **eigenes Niederschlagsschema für Stundensummen** (`precipitation-1h-vsup`). Die
  Grenzen sind eine fachliche Entscheidung (siehe 5.). Bewölkung, Wind und Temperatur bleiben.
  Die Startprüfung verlangt für jeden angebotenen Schritt ein passendes Niederschlagsschema.
- API: Parameter `step_hours`, mit 422 für einen Schritt, den das Produkt nicht anbietet.
- Frontend: ein Umschalter „6 h / 1 h“ neben „Waagerecht | Senkrecht“, nur für stündliche Produkte,
  in der URL. Die Zeitachse beschriftet dann alle 3 Stunden. Waagerecht scrollt die Zeile ohnehin
  schon, senkrecht wird die Spalte entsprechend länger.
- Tests: aufgezeichnete stündliche Vorhersage, Summenfenster `t+1 … t+1 h` gegen die Roh-Member
  (wie heute für 6 h), Screenshots in beiden Darstellungen.

**Zum Testen:** Ist die Stunde auf dem Handy lesbar? Sind die Regengrenzen pro Stunde sinnvoll?

### Stufe 5 – Weitere globale Modelle (optional)

- `ecmwf_aifs025` (KI, 51 Member) als Vergleich zum IFS. `gfs_seamless` und `gem_global` für 16 Tage.
- Vorher prüfen: Bei 6-stündlichen Modellen liegen die Modellzeiten auf 00/06/12/18 UTC, unsere
  Schritte aber auf Mitternacht **Ortszeit**. Die Niederschlagsfenster schneiden dann die Modellschritte,
  und wie Open-Meteo die Summen auf die Stunden verteilt, entscheidet über das Ergebnis.
  Gegen die Roh-Member prüfen, bevor ein solches Produkt angeboten wird.

### Stufe 6 – Modelle aneinandersetzen (optional, eher nicht)

Ein Meteogramm, das die ersten Tage aus ICON-D2 und den Rest aus ECMWF zeigt. Nicht empfohlen. An der
Naht wechseln Member-Zahl und Streuung, und das Bild zeigte einen Sprung in der Sicherheit, der nur
vom Modellwechsel kommt. Wenn überhaupt, dann mit deutlich markierter Naht. Entscheidung nach Stufe 3.

## 5. Offene Entscheidungen

| Frage | Vorschlag | Wann |
|---|---|---|
| Grenzen für stündlichen Niederschlag | 0,1 / 0,5 / 2 mm pro Stunde; die gängige internationale Einteilung (leicht bis 2,5 mm/h, mäßig bis 10 mm/h) ergäbe bei Ensemble-Werten kaum je „stark“ | vor Stufe 4 |
| Mindestzahl Member | 10; das UK-Modell (3) entfällt | Stufe 2 |
| „Besser passend“ | feinstes Gitter, das Ort und Zeitraum abdeckt; keine Bewertung nach Vorhersagegüte | Stufe 3 |
| Modellwechsel mit dem Tage-Regler | ja, mit Hinweis; Alternative: fester Standard pro Ort | nach Test von Stufe 3 |
| „Sicher“ bei kleinen Ensembles | Schwelle bleibt zwei Drittel; Verteilung je Produkt mit `vsup check` ansehen | Stufe 1/2 |
| Rate-Limits | mehr Modelle heißt mehr Abrufe; Cache 1 h pro Quelle und Ort. Für die automatische Wahl genügt der Vorfilter, kein Abruf auf Verdacht | Stufe 3 |

## 6. Aufwand (grob)

| Stufe | Aufwand |
|---|---|
| 1 Zweites globales Modell | 1–2 Tage |
| 2 Regionale Modelle, Abdeckung | 2–3 Tage |
| 3 Automatische Vorauswahl | 2 Tage |
| 4 Stündliche Schritte | 3–4 Tage |
| 5 Weitere globale Modelle | je ½ Tag nach der Prüfung |
