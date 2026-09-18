"""Forecast data and meteogram rendering.

Independent of the Flask layer in app/: downloadJsonData fetches and reduces the
ensemble, plotMeteogram turns the result into a figure, and pictogram/ holds the
VSUP glyphs that the renderer loads. Importable as `meteogram.…` from anywhere,
and the pictogram lookups are resolved relative to this package rather than the
working directory.
"""
