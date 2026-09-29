"""A deliberately small unit table: which units exist and how to convert them.

There are only a handful of quantities, so this is a lookup rather than a
general unit library. What matters is that a conversion is either known exactly
or refused - an unknown unit is an error, never passed through.

Every conversion here is affine and strictly increasing. vsup relies on that:
it converts the thresholds of a scheme written in a non-canonical unit once, at
load time, and `value < threshold` keeps its meaning only for increasing maps.
"""
import numpy as np

from core.variables import variable


class UnitError(ValueError):
    pass


# (from, to) -> (scale, offset), meaning to = from * scale + offset.
_CONVERSIONS = {
    ("km/h", "m/s"): (1 / 3.6, 0.0),
    ("kn", "m/s"): (1852 / 3600, 0.0),
    ("mph", "m/s"): (0.44704, 0.0),
    ("degF", "degC"): (5 / 9, -32 * 5 / 9),
    ("K", "degC"): (1.0, -273.15),
    ("inch", "mm"): (25.4, 0.0),
    ("cm", "mm"): (10.0, 0.0),
    ("fraction", "percent"): (100.0, 0.0),
}
assert all(scale > 0 for scale, _ in _CONVERSIONS.values()), "conversions must be increasing"

UNITS = frozenset({unit for pair in _CONVERSIONS for unit in pair})


def converter(from_unit, to_unit):
    """A function mapping values (floats or arrays) from one unit to another."""
    if from_unit == to_unit:
        return lambda values: values
    try:
        scale, offset = _CONVERSIONS[(from_unit, to_unit)]
    except KeyError:
        raise UnitError(f"cannot convert {from_unit!r} to {to_unit!r}") from None
    return lambda values: values * scale + offset


def to_canonical(values, unit, name):
    """`values` of variable `name`, given in `unit`, in the variable's canonical unit."""
    canonical = variable(name).unit
    try:
        convert = converter(unit, canonical)
    except UnitError:
        raise UnitError(
            f"{name} arrived in {unit!r}, which cannot be converted to its canonical "
            f"unit {canonical!r}"
        ) from None
    return convert(np.asarray(values, dtype=np.float64))
