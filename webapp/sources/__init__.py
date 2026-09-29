"""Adapters that fetch forecasts and hand them over as a core.pipeline.SourceResult.

Everything source-specific ends here: an adapter converts to canonical units,
to UTC, and to its SourceResult kind, and refuses what it cannot convert. The
pipeline after it is the same for every source.
"""
