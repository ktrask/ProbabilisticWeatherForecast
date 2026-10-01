"""Builds bericht.html, the source of Bericht_Probabilistische_Meteogramme.pdf.

The charts are drawn here as inline SVG from the test fixtures and the recorded
live data in data/; the pictograms in the scenario figures are chosen by the
app's own classifier (vsup/config.py) with the shipped config/vsup.yaml. The
meteogram screenshots in img/ come from `render.mjs shots`.

    webapp/.venv/bin/python docs/bericht/build.py
"""
import html
import json
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path
from string import Template
from zoneinfo import ZoneInfo

import numpy as np

HERE = Path(__file__).resolve().parent
WEBAPP = HERE.parent.parent / "webapp"
sys.path.insert(0, str(WEBAPP))

from vsup import config as vsup_config  # noqa: E402

CFG = vsup_config.load()
FIXTURES = WEBAPP / "tests" / "fixtures"
PLACES = ["braunschweig", "alice_springs", "reykjavik", "singapore", "zermatt"]
NAMES = {
    "braunschweig": "Braunschweig",
    "alice_springs": "Alice Springs",
    "reykjavik": "Reykjavík",
    "singapore": "Singapur",
    "zermatt": "Zermatt",
}
QUANTILES = ["p0", "p10", "p17", "p25", "p50", "p75", "p83", "p90", "p100"]
SCHEMES = {"cloud_cover": "cloud-vsup", "precipitation": "precipitation-vsup", "wind_speed_10m": "wind-vsup"}

# The app's colours (frontend/src/styles.css) and the dataviz categorical order.
INK, MUTED, LINE, ACCENT = "#17313a", "#5b7078", "#d6e0e3", "#004e5e"
BAND_OUTER, BAND_MIDDLE, BAND_INNER, MEDIAN = "#cfe6e8", "#8fc3c8", "#4f9aa3", "#003a46"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
# Noto Sans has no minus sign or ≥ here; name the fallback rather than leave it to chance.
FONT = "Noto Sans, Arimo, sans-serif"

LEVELS = {3: "sicher", 2: "wahrscheinlich", 1: "unsicher"}
CLASS_LABELS = {
    "cloud_cover": {"clear": "klar", "light": "leicht bewölkt", "cloudy": "bewölkt", "overcast": "bedeckt",
                    "clear+light": "klar oder leicht bewölkt", "cloudy+overcast": "bewölkt oder bedeckt"},
    "precipitation": {"none": "kein Regen", "light": "leichter Regen", "medium": "mäßiger Regen",
                      "heavy": "starker Regen", "none+light": "kein oder leichter Regen",
                      "medium+heavy": "mäßiger oder starker Regen"},
    "wind_speed_10m": {"calm": "windstill", "light": "leichter Wind", "strong": "starker Wind", "storm": "Sturm",
                       "calm+light": "windstill oder leichter Wind", "strong+storm": "starker Wind oder Sturm"},
}


def esc(text):
    return html.escape(str(text), quote=True)


def de(value, digits=0):
    """A number the German way: 0,1 - and a real minus sign."""
    text = f"{value:.{digits}f}".replace(".", ",")
    return text.replace("-", "−")


def class_label(variable, cls):
    return CLASS_LABELS[variable].get(cls, "alles möglich")


def picto(relative):
    return (CFG.pictogram_root / relative).as_uri()


def fixture(key):
    return json.loads((FIXTURES / f"{key}.json").read_text())


def classify(variable, values):
    return CFG.scheme(SCHEMES[variable]).classify(dict(zip(QUANTILES, values)))


def text(x, y, content, size=10, fill=INK, anchor="start", weight=400, extra=""):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-family="{FONT}" font-size="{size}" fill="{fill}" '
            f'text-anchor="{anchor}" font-weight="{weight}" {extra}>{esc(content)}</text>')


def svg(width, height, body, label):
    return (f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="{esc(label)}" '
            f'xmlns="http://www.w3.org/2000/svg">{"".join(body)}</svg>')


# --- Scenario figures (section 6) ---------------------------------------------

# (title, the nine quantiles p0..p100, the level the classifier must choose)
SCENARIOS = {
    "precipitation": [
        ("Alle Member trocken", [0, 0, 0, 0, 0, 0, 0, 0, 0.05], 3),
        ("Leichter Regen, einig", [0.05, 0.15, 0.2, 0.3, 0.5, 0.7, 0.8, 0.9, 1.4], 3),
        ("Mäßiger Regen, sehr einig", [0.9, 1.1, 1.2, 1.3, 1.5, 1.7, 1.8, 1.9, 2.3], 3),
        ("Starker Regen, einig", [1.2, 2.2, 2.6, 3, 4, 5.5, 6.2, 7, 11], 3),
        ("Nass, aber uneinig über die Menge", [0.6, 1.0, 1.2, 1.4, 2.2, 3.0, 3.5, 4.2, 6.5], 2),
        ("Meist trocken, einzelne Schauer", [0, 0, 0, 0, 0.05, 0.4, 0.9, 1.6, 4.0], 2),
        ("Kleiner Median, nasses oberes Viertel", [0, 0, 0, 0.02, 0.3, 1.8, 2.5, 3.2, 7], 1),
    ],
    "cloud_cover": [
        ("Wolkenlos, einig", [0, 0, 1, 2, 4, 6, 7, 9, 25], 3),
        ("Leicht bewölkt, sehr einig", [5, 15, 20, 25, 30, 38, 42, 46, 60], 3),
        ("Bewölkt, einig", [40, 55, 60, 65, 72, 80, 84, 88, 97], 3),
        ("Bedeckt, einig", [70, 92, 95, 97, 99, 100, 100, 100, 100], 3),
        ("Stark bewölkt, auf der 90-%-Grenze", [50, 73, 76, 80, 88, 94, 95, 97, 99], 2),
        ("Eher heiter", [0, 0, 2, 4, 12, 36, 63, 73, 96], 2),
        ("Mittlere Hälfte um 50 %", [0, 22, 33, 37, 59, 72, 76, 81, 95], 1),
    ],
    "wind_speed_10m": [
        ("Windstill, einig", [0.5, 1.0, 1.2, 1.4, 1.8, 2.2, 2.4, 2.7, 3.5], 3),
        ("Leichter Wind, einig", [2.5, 3.8, 4.2, 4.8, 6, 7.2, 7.8, 8.5, 11], 3),
        ("Starker Wind, einig", [8, 10.5, 11.5, 12, 13.5, 15, 15.8, 16.5, 19], 3),
        ("Sturm, einig", [14, 17.5, 18.5, 19.5, 21, 23, 24, 25, 29], 3),
        ("Eher stark, bis in den Sturm", [5, 8, 10.5, 11.5, 14, 16.5, 18, 20, 24], 2),
        ("Eher schwach", [1, 2, 2.6, 3.2, 4.5, 6.5, 8, 9.5, 12], 2),
        ("Von Flaute bis Sturm", [1.5, 3, 4.5, 6, 9.5, 13, 15, 17, 22], 1),
    ],
}
AXES = {
    # domain max, sqrt scale?, ticks, unit
    "precipitation": (12, True, [0, 0.1, 0.5, 1, 2, 5, 10], "mm in 6 h"),
    "cloud_cover": (100, False, [0, 25, 50, 75, 100], "% Himmelsbedeckung"),
    "wind_speed_10m": (30, False, [0, 5, 10, 15, 20, 25, 30], "m/s"),
}


def scenario_figure(variable):
    scheme = CFG.scheme(SCHEMES[variable])
    top_max, sqrt_scale, ticks, unit = AXES[variable]
    W, X0, X1, PIC = 700, 222, 528, 540
    TOP, RH = 52, 46
    rows = SCENARIOS[variable]
    height = TOP + RH * len(rows) + 34

    def x(v):
        f = math.sqrt(max(v, 0) / top_max) if sqrt_scale else max(v, 0) / top_max
        return X0 + (X1 - X0) * f

    def fmt(v):
        return de(v, 0 if float(v).is_integer() else 1)

    body = []
    bounds = list(scheme.bounds)
    edges = [0, *bounds, top_max]
    for i, cls in enumerate(scheme.class_ids):
        mid = (x(edges[i]) + x(edges[i + 1])) / 2
        body.append(text(mid, 11 + 12 * (i % 2), class_label(variable, cls), 9, MUTED, "middle"))
    bottom = TOP + RH * len(rows)
    for b in bounds:
        body.append(f'<line x1="{x(b):.1f}" x2="{x(b):.1f}" y1="{TOP - 15}" y2="{bottom}" stroke="{MUTED}" '
                    f'stroke-width="1" stroke-dasharray="3 3"/>')
        body.append(text(x(b), TOP - 19, fmt(b), 9, INK, "middle", 600))
    for r, (title, values, expected) in enumerate(rows):
        choice = classify(variable, values)
        assert choice.level == expected, (variable, title, choice)
        q = dict(zip(QUANTILES, values))
        cy = TOP + r * RH + RH / 2
        if r:
            body.append(f'<line x1="0" x2="{W}" y1="{TOP + r * RH}" y2="{TOP + r * RH}" stroke="#edf2f3"/>')
        body.append(text(0, cy - 3, f"{chr(65 + r)}", 11, ACCENT, "start", 700))
        body.append(text(16, cy - 3, title, 10, INK, "start", 600))
        body.append(text(16, cy + 11, f"p17–p83: {fmt(q['p17'])}–{fmt(q['p83'])} · p25–p75: {fmt(q['p25'])}–{fmt(q['p75'])}",
                         8.5, MUTED))
        body.append(f'<line x1="{x(q["p0"]):.1f}" x2="{x(q["p100"]):.1f}" y1="{cy}" y2="{cy}" stroke="{BAND_MIDDLE}" stroke-width="1.5"/>')
        for end in ("p0", "p100"):
            body.append(f'<line x1="{x(q[end]):.1f}" x2="{x(q[end]):.1f}" y1="{cy - 4}" y2="{cy + 4}" stroke="{BAND_MIDDLE}" stroke-width="1.5"/>')
        body.append(f'<rect x="{x(q["p17"]):.1f}" y="{cy - 8}" width="{max(x(q["p83"]) - x(q["p17"]), 1.5):.1f}" height="16" '
                    f'rx="2" fill="{BAND_MIDDLE}"/>')
        body.append(f'<rect x="{x(q["p25"]):.1f}" y="{cy - 8}" width="{max(x(q["p75"]) - x(q["p25"]), 1.5):.1f}" height="16" '
                    f'rx="2" fill="{BAND_INNER}"/>')
        body.append(f'<line x1="{x(q["p50"]):.1f}" x2="{x(q["p50"]):.1f}" y1="{cy - 11}" y2="{cy + 11}" stroke="{MEDIAN}" stroke-width="2.5"/>')
        body.append(f'<image href="{picto(choice.pictogram)}" x="{PIC}" y="{cy - 17}" width="34" height="34"/>')
        body.append(text(PIC + 42, cy - 3, f"Stufe {choice.level} · {LEVELS[choice.level]}", 9.5, INK, "start", 600))
        body.append(text(PIC + 42, cy + 11, class_label(variable, choice.class_), 8.5, MUTED))
    body.append(f'<line x1="{X0}" x2="{X1}" y1="{bottom}" y2="{bottom}" stroke="{LINE}"/>')
    for t in ticks:
        body.append(f'<line x1="{x(t):.1f}" x2="{x(t):.1f}" y1="{bottom}" y2="{bottom + 4}" stroke="{MUTED}"/>')
        body.append(text(x(t), bottom + 15, fmt(t), 9, MUTED, "middle"))
    body.append(text((X0 + X1) / 2, bottom + 29, unit + (" (Wurzelskala)" if sqrt_scale else ""), 9, MUTED, "middle"))
    return svg(W, height, body, f"Szenarien {variable}")


# --- Temperature band (section 4) ---------------------------------------------

def local_times(forecast):
    tz = ZoneInfo(forecast["location"]["timezone"])
    return [datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(tz) for s in forecast["steps"]]


def band_figure(key="zermatt"):
    f = fixture(key)
    q = {k: np.array(v) for k, v in f["variables"]["temperature_2m"]["quantiles"].items()}
    times = local_times(f)
    n = len(times)
    W, H, L, R, T, B = 700, 270, 40, 12, 40, 30
    lo, hi = math.floor(q["p0"].min() / 5) * 5, math.ceil(q["p100"].max() / 5) * 5
    t0, t1 = times[0], times[-1]

    def x(t):
        return L + (W - L - R) * (t - t0).total_seconds() / (t1 - t0).total_seconds()

    def y(v):
        return T + (H - T - B) * (hi - v) / (hi - lo)

    def area(a, b):
        pts = [f"{x(times[i]):.1f},{y(a[i]):.1f}" for i in range(n)]
        pts += [f"{x(times[i]):.1f},{y(b[i]):.1f}" for i in reversed(range(n))]
        return " ".join(pts)

    body = []
    for v in range(lo, hi + 1, 5):
        body.append(f'<line x1="{L}" x2="{W - R}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="{LINE}" stroke-width="0.8"/>')
        body.append(text(L - 6, y(v) + 3, de(v) + (" °C" if v == hi else ""), 9, MUTED, "end"))
    day = t0
    k = 0
    while day <= t1:
        body.append(f'<line x1="{x(day):.1f}" x2="{x(day):.1f}" y1="{T}" y2="{H - B}" stroke="{LINE}" stroke-width="0.8"/>')
        if k % 2 == 0:
            body.append(text(x(day) + 3, H - B + 14, f"{day.day}.{day.month}.", 9, MUTED))
        day = (day + timedelta(days=1, hours=2)).replace(hour=0)
        k += 1
    for a, b, colour in (("p0", "p100", BAND_OUTER), ("p10", "p90", BAND_MIDDLE), ("p25", "p75", BAND_INNER)):
        body.append(f'<polygon points="{area(q[a], q[b])}" fill="{colour}"/>')
    median = " ".join(f"{x(times[i]):.1f},{y(q['p50'][i]):.1f}" for i in range(n))
    body.append(f'<polyline points="{median}" fill="none" stroke="{MEDIAN}" stroke-width="2"/>')
    width = q["p90"] - q["p10"]
    for i, anchor in ((2, "start"), (int(width.argmax()), "end")):
        xi = x(times[i])
        body.append(f'<line x1="{xi:.1f}" x2="{xi:.1f}" y1="{y(q["p90"][i]):.1f}" y2="{y(q["p10"][i]):.1f}" '
                    f'stroke="{INK}" stroke-width="1.2"/>')
        for v in (q["p90"][i], q["p10"][i]):
            body.append(f'<line x1="{xi - 4:.1f}" x2="{xi + 4:.1f}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="{INK}" stroke-width="1.2"/>')
        dx = 8 if anchor == "start" else -8
        label = f"{times[i].day}.{times[i].month}., {times[i]:%H} Uhr: {de(width[i], 1)} K"
        body.append(text(xi + dx, y(q["p100"][i]) - 8, label, 9.5, INK, anchor, 600))
    legend = [("alle 51 Member (p0–p100)", BAND_OUTER), ("80 % der Member (p10–p90)", BAND_MIDDLE),
              ("mittlere Hälfte (p25–p75)", BAND_INNER)]
    lx = L
    for name, colour in legend:
        body.append(f'<rect x="{lx}" y="8" width="14" height="10" rx="2" fill="{colour}"/>')
        body.append(text(lx + 19, 17, name, 9.5, INK))
        lx += 19 + 5.4 * len(name) + 22
    body.append(f'<line x1="{lx}" x2="{lx + 16}" y1="13" y2="13" stroke="{MEDIAN}" stroke-width="2"/>')
    body.append(text(lx + 21, 17, "Median", 9.5, INK))
    return svg(W, H, body, "Temperaturband Zermatt")


# --- Spread over lead time (section 8) ----------------------------------------

def spread_figure():
    W, H, L, R, T, B = 700, 290, 40, 104, 40, 38
    series = []
    for key in PLACES:
        f = fixture(key)
        q = f["variables"]["temperature_2m"]["quantiles"]
        width = np.array(q["p90"]) - np.array(q["p10"])
        smooth = np.convolve(width, np.ones(4) / 4, mode="valid")  # one day of 6-hour steps
        lead = (np.arange(len(smooth)) + 1.5) * f["step_hours"] / 24
        series.append((key, lead, smooth))
    xmax = 15
    ymax = math.ceil(max(s.max() for _, _, s in series) / 2) * 2

    def x(v):
        return L + (W - L - R) * v / xmax

    def y(v):
        return T + (H - T - B) * (ymax - v) / ymax

    body = []
    for v in range(0, ymax + 1, 2):
        body.append(f'<line x1="{L}" x2="{W - R}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="{LINE}" stroke-width="0.8"/>')
        body.append(text(L - 6, y(v) + 3, de(v) + (" K" if v == ymax else ""), 9, MUTED, "end"))
    for d in range(0, xmax + 1, 1):
        body.append(f'<line x1="{x(d):.1f}" x2="{x(d):.1f}" y1="{H - B}" y2="{H - B + 4}" stroke="{MUTED}"/>')
        if d % 2 == 0:
            body.append(text(x(d), H - B + 15, de(d), 9, MUTED, "middle"))
    body.append(text((L + W - R) / 2, H - 6, "Vorhersagezeit in Tagen", 9.5, MUTED, "middle"))
    ends = []
    for (key, lead, smooth), colour in zip(series, SERIES):
        pts = " ".join(f"{x(a):.1f},{y(b):.1f}" for a, b in zip(lead, smooth))
        body.append(f'<polyline points="{pts}" fill="none" stroke="#ffffff" stroke-width="4" stroke-linejoin="round"/>')
        body.append(f'<polyline points="{pts}" fill="none" stroke="{colour}" stroke-width="2" stroke-linejoin="round"/>')
        ends.append([y(smooth[-1]), key, colour, x(lead[-1])])
    ends.sort()
    for i in range(1, len(ends)):  # keep the end labels apart
        ends[i][0] = max(ends[i][0], ends[i - 1][0] + 12)
    for ly, key, colour, lx in ends:
        body.append(text(lx + 6, ly + 3.5, NAMES[key], 9.5, INK))
    lx = L
    for (key, _, _), colour in zip(series, SERIES):
        body.append(f'<line x1="{lx}" x2="{lx + 16}" y1="13" y2="13" stroke="{colour}" stroke-width="2.5"/>')
        body.append(text(lx + 21, 17, NAMES[key], 9.5, INK))
        lx += 21 + 6.3 * len(NAMES[key]) + 20
    body.append(text(L, 33, "Breite des 10.–90.-Perzentil-Bands der Temperatur, gleitendes Tagesmittel", 9, MUTED))
    return svg(W, H, body, "Temperaturspreizung über die Vorhersagezeit")


# --- Döteberg: members per cloud class (section 10) -----------------------------

CLOUD_RAMP = ["#e2eff1", "#a9d0d4", "#4f9aa3", "#1d5561"]


def members_figure():
    raw = json.loads((HERE / "data" / "doeteberg_ensemble.json").read_text())
    app = json.loads((HERE / "data" / "doeteberg_app.json").read_text())
    hourly = raw["hourly"]
    keys = ["cloud_cover"] + [f"cloud_cover_member{i:02d}" for i in range(1, 51)]
    members = np.array([hourly[k] for k in keys], dtype=float)
    times = [datetime.fromisoformat(t) for t in hourly["time"]]
    start, end = datetime(2026, 9, 29, 18), datetime(2026, 10, 1, 6)
    picks = [i for i, t in enumerate(times) if start <= t <= end and t.hour % 3 == 0]
    bounds = list(CFG.scheme("cloud-vsup").bounds)
    tz = ZoneInfo(app["location"]["timezone"])
    app_steps = {datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(tz).replace(tzinfo=None): i
                 for i, s in enumerate(app["steps"])}

    W, H, L, T = 700, 330, 36, 40
    bar_bottom = 214
    slot = (W - L - 10) / len(picks)
    bw = slot * 0.62

    def y(count):
        return bar_bottom - (bar_bottom - T) * count / 51

    body = []
    for c in (0, 17, 34, 51):
        body.append(f'<line x1="{L}" x2="{W - 10}" y1="{y(c):.1f}" y2="{y(c):.1f}" stroke="{LINE}" stroke-width="0.8"/>')
        body.append(text(L - 6, y(c) + 3, str(c), 9, MUTED, "end"))
    for j, i in enumerate(picks):
        values = members[:, i]
        counts = np.bincount(np.searchsorted(bounds, values, side="right"), minlength=4)
        cx = L + slot * (j + 0.5)
        base = 0
        for cls, count in enumerate(counts):
            if count == 0:
                continue
            y0, y1 = y(base), y(base + count)
            body.append(f'<rect x="{cx - bw / 2:.1f}" y="{y1 + 1:.1f}" width="{bw:.1f}" height="{max(y0 - y1 - 2, 0.5):.1f}" '
                        f'fill="{CLOUD_RAMP[cls]}"/>')
            if count >= 4:
                body.append(text(cx, (y0 + y1) / 2 + 3.5, str(count), 9, "#ffffff" if cls >= 2 else INK, "middle", 600))
            base += count
        t = times[i]
        body.append(text(cx, bar_bottom + 13, f"{t:%H}", 9, MUTED, "middle"))
        if t.hour == 0 or j == 0:
            body.append(text(cx, bar_bottom + 25, ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"][t.weekday()] + f" {t.day}.{t.month}.",
                             9, INK, "middle", 600))
        if t in app_steps:
            item = app["pictograms"]["cloud_cover"]["items"][app_steps[t]]
            body.append(f'<image href="{picto(item["pictogram"])}" x="{cx - 17:.1f}" y="{bar_bottom + 34}" width="34" height="34"/>')
            body.append(text(cx, bar_bottom + 80, LEVELS[item["level"]], 8.5, MUTED, "middle"))
    body.append(text(L - 30, bar_bottom + 52, "App:", 9, MUTED))
    lx = L
    for cls, colour in zip(CFG.scheme("cloud-vsup").class_ids, CLOUD_RAMP):
        name = class_label("cloud_cover", cls)
        body.append(f'<rect x="{lx}" y="8" width="14" height="10" rx="2" fill="{colour}" stroke="{LINE}" stroke-width="0.5"/>')
        body.append(text(lx + 19, 17, name, 9.5, INK))
        lx += 19 + 6.4 * len(name) + 18
    body.append(text(L, 32, "Anzahl der 51 Member je Wolkenklasse (Gesamtbewölkung, alle 3 Stunden)", 9, MUTED))
    return svg(W, H, body, "Ensemble-Member je Bewölkungsklasse in Döteberg")


def models_table():
    det = json.loads((HERE / "data" / "doeteberg_modelle.json").read_text())
    app = json.loads((HERE / "data" / "doeteberg_app.json").read_text())
    tz = ZoneInfo(app["location"]["timezone"])
    wanted = [datetime(2026, 9, 30, h) for h in (0, 6, 12, 18)] + [datetime(2026, 10, 1, 0)]
    hourly = det["hourly"]
    index = {datetime.fromisoformat(t): i for i, t in enumerate(hourly["time"])}
    models = [("cloud_cover_icon_d2", "DWD ICON-D2 (2 km)"), ("cloud_cover_icon_eu", "DWD ICON-EU (7 km)"),
              ("cloud_cover_ecmwf_ifs025", "ECMWF IFS, deterministisch"), ("cloud_cover_gfs_seamless", "NOAA GFS"),
              ("cloud_cover_meteofrance_seamless", "Météo-France")]
    head = "".join(f"<th>{['Mo','Di','Mi','Do','Fr','Sa','So'][t.weekday()]} {t:%H} Uhr</th>" for t in wanted)
    rows = []
    for key, label in models:
        cells = "".join(f"<td>{de(hourly[key][index[t]])} %</td>" for t in wanted)
        rows.append(f"<tr><td>{esc(label)}</td>{cells}</tr>")
    steps = {datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(tz).replace(tzinfo=None): i
             for i, s in enumerate(app["steps"])}
    q = app["variables"]["cloud_cover"]["quantiles"]
    cells = "".join(f"<td>{de(q['p17'][steps[t]])}–{de(q['p83'][steps[t]])} %</td>" for t in wanted)
    rows.append(f'<tr class="ens"><td>ECMWF-Ensemble, p17–p83 (App)</td>{cells}</tr>')
    return f'<table class="num"><thead><tr><th>Modell</th>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table>'


# --- Tables --------------------------------------------------------------------

def level_table():
    rows = []
    for key in PLACES:
        f = fixture(key)
        cells = []
        for variable in ("cloud_cover", "precipitation", "wind_speed_10m"):
            quantiles = f["variables"][variable]["quantiles"]
            levels = [classify(variable, [quantiles[k][i] for k in QUANTILES]).level for i in range(len(f["steps"]))]
            for level in (3, 2, 1):
                share = 100 * levels.count(level) / len(levels)
                cells.append(f'<td class="l{level}">{de(share)} %</td>')
        rows.append(f"<tr><td>{NAMES[key]}</td>{''.join(cells)}</tr>")
    sub = "".join("<th>sicher</th><th>wahrsch.</th><th>unsicher</th>" for _ in range(3))
    return ('<table class="num levels"><thead><tr><th rowspan="2">Ort</th><th colspan="3">Bewölkung</th>'
            '<th colspan="3">Niederschlag</th><th colspan="3">Wind</th></tr>'
            f'<tr>{sub}</tr></thead><tbody>{"".join(rows)}</tbody></table>')


def hres_grid():
    columns = [
        ("Niederschlag", "rain", [("KeinRegen", "kein"), ("leichterRegen", "leicht"), ("MittlererRegen", "mittel"), ("Starkregen", "stark")]),
        ("Bewölkung", "cloud", [("klarerHimmel", "klar"), ("leichtBedeckt", "leicht"), ("mittlereBewoelkung", "mittel"), ("starkBewoelkt", "stark")]),
        ("Wind", "wind", [("Windstille", "still"), ("leichterWind", "leicht"), ("starkerWind", "stark"), ("Sturm", "Sturm")]),
    ]
    head1 = "".join(f'<th colspan="4">{name}</th>' for name, _, _ in columns)
    head2 = "".join(f"<th>{label}</th>" for _, _, classes in columns for _, label in classes)
    rows = []
    for level, note in ((4, "≥ 90 % auf der HRES-Seite"), (3, "≥ 50 %"), (2, "≥ 25 %"), (1, "sonst")):
        cells = "".join(
            f'<td><img src="{picto(f"{folder}/enhanced_hres/Stufe{level}_{file}.png")}" alt=""></td>'
            for _, folder, classes in columns for file, _ in classes)
        rows.append(f"<tr><th>Stufe {level}<small>{note}</small></th>{cells}</tr>")
    return (f'<table class="hres"><thead><tr><th></th>{head1}</tr><tr><th></th>{head2}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody></table>')


def yaml_snippet(name="cloud-vsup"):
    lines = (WEBAPP / "config" / "vsup.yaml").read_text().splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == f"{name}:")
    end = next(i for i in range(start + 1, len(lines)) if not lines[i].strip())
    return "\n".join(line[2:] for line in lines[start:end])


def main():
    page = Template((HERE / "bericht.template.html").read_text())
    out = page.substitute(
        legend="img/legende.png",
        fig_band=band_figure(),
        fig_rain=scenario_figure("precipitation"),
        fig_cloud=scenario_figure("cloud_cover"),
        fig_wind=scenario_figure("wind_speed_10m"),
        fig_spread=spread_figure(),
        fig_members=members_figure(),
        table_models=models_table(),
        table_levels=level_table(),
        hres_grid=hres_grid(),
        yaml=esc(yaml_snippet()),
    )
    (HERE / "bericht.html").write_text(out)
    print(HERE / "bericht.html")


if __name__ == "__main__":
    main()
