"""VSUP schemes: which pictogram a step's quantiles get, as configuration.

Replaces the hard-coded getVSUP*Coordinate() functions and the filename lists
they are positionally coupled to. A scheme lives in config/vsup.yaml and is
checked when it is loaded - missing pictograms, unordered thresholds, a unit
that is not the variable's, quantiles that are never computed - so a broken
scheme stops the start-up instead of the first request.

    python -m vsup check            # validate config/vsup.yaml, show coverage
    python -m vsup schema           # JSON Schema for editor support

`expr` is the language of `when:` in rules-mode schemes, `config` loads and
validates, `classify` applies a scheme to a forecast.
"""
