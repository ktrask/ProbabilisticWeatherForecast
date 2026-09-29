"""The adapter interface and the errors adapters raise."""
from datetime import timedelta
from typing import Literal, Protocol

from core.model import Location
from core.pipeline import SourceResult


class SourceError(RuntimeError):
    """The source failed or sent something unusable. The request itself was fine."""


class SourceTimeout(SourceError):
    """The source did not answer in time."""


class NoData(SourceError):
    """The source works, but has nothing for this request - no fixture near
    the location, for instance."""


class ForecastSource(Protocol):
    id: str  # "open-meteo:ecmwf_ifs025"
    kind: Literal["ensemble", "quantiles"]
    variables: frozenset[str]  # what the source can deliver
    quantile_levels: tuple[int, ...] | None  # quantile sources: the levels on offer
    max_lead: timedelta
    native_step: timedelta

    async def fetch(self, location: Location, variables: set[str]) -> SourceResult: ...
