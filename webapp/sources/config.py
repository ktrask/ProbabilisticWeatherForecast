"""Loading config/sources.yaml: which sources exist, and which products are offered.

A product is what the user picks - "ECMWF ensemble" - and ties a source to the
VSUP schemes drawn from it. Loading checks, once, that every product can
actually do what it promises: each scheme exists, draws a variable the source
delivers, is written for the step the forecasts are built in, and reads only
quantiles the source can provide. What would otherwise fail on some later
request - the successor of the legacy HresDataUnavailable - fails at start-up.
"""
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.configfile import ConfigFile
from core.model import DETERMINISTIC, quantile_name
from core.pipeline import STEP_HOURS
from core.variables import VARIABLES
from sources.base import Area
from sources.fixture import DEFAULT_DIRECTORY as DEFAULT_FIXTURES
from sources.fixture import FixtureSource
from sources.open_meteo import DEFAULT_TIMEOUT_S, OpenMeteoEnsemble

# Fewer members than this, and "two thirds agree" rests on a handful of runs.
MIN_MEMBERS = 10

DEFAULT_SOURCES = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AreaSpec(_Strict):
    """A box around a regional model's domain, in degrees. Inside it the model's
    own answer decides; outside it the source is not asked."""

    south: float = Field(ge=-90, le=90)
    north: float = Field(ge=-90, le=90)
    west: float = Field(ge=-180, le=180)
    east: float = Field(ge=-180, le=180)

    @model_validator(mode="after")
    def _ordered(self):
        if self.south >= self.north:
            raise ValueError("south must be below north")
        if self.west >= self.east:
            raise ValueError("west must be west of east")
        return self


AREA_DESCRIPTION = "For a regional model: the box around its domain. Default: the whole globe."


class OpenMeteoEnsembleSpec(_Strict):
    adapter: Literal["open_meteo_ensemble"]
    model: str = Field("ecmwf_ifs025", description="Open-Meteo's name for the ensemble model.")
    forecast_days: int = Field(15, ge=1, le=35, description="How far ahead to ask; NaN past the run's end is dropped.")
    timeout_s: float = Field(DEFAULT_TIMEOUT_S, gt=0, le=120)
    area: AreaSpec | None = Field(None, description=AREA_DESCRIPTION)


class FixtureSpec(_Strict):
    adapter: Literal["fixture"]
    directory: str | None = Field(None, description="Recorded forecasts, relative to this file. Default: tests/fixtures.")
    area: AreaSpec | None = Field(None, description="As the live source it stands in for.")


class ProductSpec(_Strict):
    label: str = Field(description="What the UI calls it.")
    ensemble: str = Field(description="Key of the ensemble source under `sources`.")
    schemes: list[str] = Field(min_length=1, description="VSUP schemes from vsup.yaml, at most one per variable.")
    variables: list[Literal[tuple(VARIABLES)]] | None = Field(
        None, description="What to fetch. Default: everything the source delivers."
    )
    # What the reader is told when choosing - and, later, what an automatic
    # choice weighs. The live tests hold members and horizon to the real thing.
    members: int = Field(ge=MIN_MEMBERS, description="Ensemble members the model runs.")
    grid_km: float = Field(gt=0, description="Grid spacing of the model, in km.")
    horizon_days: float = Field(gt=0, description="About how far ahead a run reaches, in days.")
    automatic: bool = Field(
        True, description="Whether the automatic choice may take it (sources/choice.py); it stays selectable by hand."
    )
    steps: list[int] = Field(
        [STEP_HOURS], min_length=1,
        description="Step widths in hours it can be drawn in, the first the default. Each must divide a day; "
                    "offer 1 only for a model that really computes every hour.",
    )

    @field_validator("steps")
    @classmethod
    def _steps_divide_a_day(cls, steps):
        for step in steps:
            if step < 1 or 24 % step:
                raise ValueError(f"a step of {step} hours does not divide a day")
        if len(set(steps)) != len(steps):
            raise ValueError("a step is listed twice")
        return steps


class SourcesFile(_Strict):
    version: Literal[1]
    sources: dict[str, Annotated[OpenMeteoEnsembleSpec | FixtureSpec, Field(discriminator="adapter")]] = Field(
        min_length=1
    )
    products: dict[str, ProductSpec] = Field(min_length=1, description="The first one is the default.")


def json_schema():
    schema = SourcesFile.model_json_schema()
    schema["title"] = "Forecast sources and products"
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return schema


@dataclass(frozen=True)
class Product:
    id: str
    label: str
    source_key: str
    source: object
    schemes: tuple
    variables: frozenset
    members: int
    grid_km: float
    horizon_days: float
    automatic: bool
    steps: tuple  # hours, the first the default
    windows: tuple = ()  # (scheme name, window_hours or None), as in `schemes`

    @property
    def area(self):
        return self.source.area if self.source is not None else None

    def schemes_for(self, step_hours):
        """The schemes that draw a forecast in `step_hours` steps: every instant
        scheme, and the totals written for exactly that window."""
        return [name for name, window in self.windows if window in (None, step_hours)]

    # "hres" joins once a product can have a deterministic source.
    variants = ("ensemble",)


@dataclass(frozen=True)
class Catalog:
    path: Path
    sources: dict
    products: dict  # in file order; the first is the default

    @property
    def default(self):
        return next(iter(self.products.values()))

    def product(self, key):
        try:
            return self.products[key]
        except KeyError:
            raise KeyError(f"no product {key!r}; there are {', '.join(self.products)}") from None


def load(path, vsup_config):
    """Load `path` and check every product against `vsup_config`. Raises
    ConfigError listing every problem found."""
    file = ConfigFile(path, SourcesFile, union_tags={"sources": ("open_meteo_ensemble", "fixture")})
    sources = {key: _adapter(file, key, spec) for key, spec in file.spec.sources.items()}
    for key, source in sources.items():
        if source is not None:
            _check_source(file, key, source, vsup_config)
    products = {
        key: _product(file, key, spec, sources, vsup_config) for key, spec in file.spec.products.items()
    }
    file.check()
    return Catalog(file.path, sources, products)


def _adapter(file, key, spec):
    area = Area(spec.area.south, spec.area.north, spec.area.west, spec.area.east) if spec.area else None
    if spec.adapter == "open_meteo_ensemble":
        return OpenMeteoEnsemble(spec.model, forecast_days=spec.forecast_days, timeout_s=spec.timeout_s, area=area)
    directory = (file.path.parent / spec.directory).resolve() if spec.directory else DEFAULT_FIXTURES
    try:
        return FixtureSource(directory, area=area)
    except OSError as exc:
        file.report(("sources", key, "directory"), f"cannot read recorded forecasts from {directory}: {exc.strerror}")
        return None


def _check_source(file, key, source, vsup_config):
    if source.quantile_levels is None:
        return
    missing = [q for q in vsup_config.quantiles if q not in source.quantile_levels]
    if missing:
        file.report(("sources", key), f"offers only the quantiles {list(source.quantile_levels)}, but "
                                      f"{vsup_config.path.name} computes {', '.join(map(quantile_name, missing))} too")


def _product(file, key, spec, sources, vsup_config):
    at = ("products", key)
    source = sources.get(spec.ensemble)
    if spec.ensemble not in sources:
        file.report(at + ("ensemble",), f"no source {spec.ensemble!r}; there are {', '.join(sources)}")
    offered = source.variables if source is not None else frozenset(VARIABLES)
    variables = frozenset(spec.variables) if spec.variables is not None else offered
    unavailable = sorted(variables - offered)
    if unavailable:
        file.report(at + ("variables",), f"{spec.ensemble} does not deliver {', '.join(unavailable)}")

    for i, step in enumerate(spec.steps):
        if source is not None and not _can_build(source, step):
            file.report(at + ("steps", i), f"{spec.ensemble} cannot deliver {step}-hour steps")

    drawn = {}  # (variable, window) -> scheme name
    windows = []
    for i, name in enumerate(spec.schemes):
        loc = at + ("schemes", i)
        scheme = vsup_config.schemes.get(name)
        if scheme is None:
            file.report(loc, f"no scheme {name!r} in {vsup_config.path.name}")
            continue
        windows.append((name, scheme.window_hours))
        if scheme.variable not in variables:
            file.report(loc, f"{name} draws {scheme.variable}, which this product does not fetch")
        at_window = (scheme.variable, scheme.window_hours)
        if at_window in drawn:
            file.report(loc, f"{name} and {drawn[at_window]} both draw {scheme.variable}")
        drawn[at_window] = name
        if scheme.window_hours is not None and scheme.window_hours not in spec.steps:
            steps = " or ".join(map(str, spec.steps))
            file.report(loc, f"{name} is written for {scheme.window_hours}-hour totals, but this product "
                             f"is drawn in {steps}-hour steps")
        if DETERMINISTIC in scheme.reads:
            file.report(loc, f"{name} reads the deterministic run, which an ensemble product does not have")
    # A total drawn in one step width has to be drawn in every one the product offers.
    totals = {variable for variable, window in drawn if window is not None}
    for variable in sorted(totals):
        for step in spec.steps:
            if (variable, step) not in drawn:
                file.report(at + ("schemes",), f"no scheme draws {variable} in {step}-hour steps, which this "
                                               f"product offers")
    return Product(key, spec.label, spec.ensemble, source, tuple(spec.schemes), variables,
                   spec.members, spec.grid_km, spec.horizon_days, spec.automatic, tuple(spec.steps), tuple(windows))


def _can_build(source, step_hours):
    """Recorded quantiles only in the steps they were recorded in; members in
    any multiple of their own rows."""
    recorded = getattr(source, "steps", None)
    if recorded is not None:
        return step_hours in recorded
    return timedelta(hours=step_hours) % source.native_step == timedelta(0)
