"""The data model and the arithmetic shared by every source and every consumer.

Knows nothing about HTTP or rendering: `variables` and `units` say what a
forecast may contain and in which units, `model` is the forecast as it leaves
the backend, `reduce` turns ensemble members into quantiles on the step grid,
and `pipeline` puts a source's result through all of that. `configfile` is the
YAML loading that vsup/ and sources/ share.

Successor of the legacy downloadJsonData's reduction and of its allMeteogramData
format; core.legacy still reads that format, for the fixtures recorded in it.
"""
