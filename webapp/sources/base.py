"""The adapter interface, the errors adapters raise, and the area a regional
model covers."""
from dataclasses import dataclass
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


class NotCovered(NoData):
    """The location lies outside the area the source's model computes."""


class UpstreamStatus(SourceError):
    """The upstream service answered, but not with 200."""

    def __init__(self, message, status, reason):
        super().__init__(message)
        self.status = status
        self.reason = reason


@dataclass(frozen=True)
class Area:
    """A latitude/longitude box around a regional model's domain. Domains on a
    rotated grid fill it only partly: inside it the model's own answer decides,
    outside it there is no need to ask."""

    south: float
    north: float
    west: float
    east: float

    def contains(self, lat, lon):
        return self.south <= lat <= self.north and self.west <= lon <= self.east

    @property
    def center(self):
        return (self.south + self.north) / 2, (self.west + self.east) / 2


class ForecastSource(Protocol):
    id: str  # "open-meteo:ecmwf_ifs025"
    kind: Literal["ensemble", "quantiles"]
    variables: frozenset[str]  # what the source can deliver
    quantile_levels: tuple[int, ...] | None  # quantile sources: the levels on offer
    max_lead: timedelta
    native_step: timedelta
    area: Area | None  # None: the whole globe

    def probe(self, location: Location) -> Location: ...  # where a health check asks

    # step_hours: what quantile sources need to pick a recording; member sources ignore it.
    async def fetch(self, location: Location, variables: set[str], step_hours: int | None = None) -> SourceResult: ...
