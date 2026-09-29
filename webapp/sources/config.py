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

from pydantic import BaseModel, ConfigDict, Field

from core.configfile import ConfigFile
from core.model import DETERMINISTIC, quantile_name
from core.pipeline import STEP_HOURS
from core.variables import VARIABLES
from sources.fixture import DEFAULT_DIRECTORY as DEFAULT_FIXTURES
from sources.fixture import FixtureSource
from sources.open_meteo import DEFAULT_TIMEOUT_S, OpenMeteoEnsemble

DEFAULT_SOURCES = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OpenMeteoEnsembleSpec(_Strict):
    adapter: Literal["open_meteo_ensemble"]
    model: str = Field("ecmwf_ifs025", description="Open-Meteo's name for the ensemble model.")
    forecast_days: int = Field(15, ge=1, le=35, description="How far ahead to ask; NaN past the run's end is dropped.")
    timeout_s: float = Field(DEFAULT_TIMEOUT_S, gt=0, le=120)


class FixtureSpec(_Strict):
    adapter: Literal["fixture"]
    directory: str | None = Field(None, description="Recorded forecasts, relative to this file. Default: tests/fixtures.")


class ProductSpec(_Strict):
    label: str = Field(description="What the UI calls it.")
    ensemble: str = Field(description="Key of the ensemble source under `sources`.")
    schemes: list[str] = Field(min_length=1, description="VSUP schemes from vsup.yaml, at most one per variable.")
    variables: list[Literal[tuple(VARIABLES)]] | None = Field(
        None, description="What to fetch. Default: everything the source delivers."
    )


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
    if spec.adapter == "open_meteo_ensemble":
        return OpenMeteoEnsemble(spec.model, forecast_days=spec.forecast_days, timeout_s=spec.timeout_s)
    directory = (file.path.parent / spec.directory).resolve() if spec.directory else DEFAULT_FIXTURES
    try:
        return FixtureSource(directory)
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
    if source.native_step != timedelta(hours=STEP_HOURS):
        file.report(("sources", key), f"delivers {source.native_step} steps; quantiles cannot be re-gridded "
                                      f"to the {STEP_HOURS}-hour steps forecasts are built in")


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

    drawn = {}
    for i, name in enumerate(spec.schemes):
        loc = at + ("schemes", i)
        scheme = vsup_config.schemes.get(name)
        if scheme is None:
            file.report(loc, f"no scheme {name!r} in {vsup_config.path.name}")
            continue
        if scheme.variable not in variables:
            file.report(loc, f"{name} draws {scheme.variable}, which this product does not fetch")
        if scheme.variable in drawn:
            file.report(loc, f"{name} and {drawn[scheme.variable]} both draw {scheme.variable}")
        drawn[scheme.variable] = name
        if scheme.window_hours is not None and scheme.window_hours != STEP_HOURS:
            file.report(loc, f"{name} is written for {scheme.window_hours}-hour totals, but forecasts "
                             f"are built in {STEP_HOURS}-hour steps")
        if DETERMINISTIC in scheme.reads:
            file.report(loc, f"{name} reads the deterministic run, which an ensemble product does not have")
    return Product(key, spec.label, spec.ensemble, source, tuple(spec.schemes), variables)
