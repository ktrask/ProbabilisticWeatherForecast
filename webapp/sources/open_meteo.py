"""Open-Meteo's ensemble API - successor of meteogram.downloadJsonData.getData.

Fetches over httpx with an explicit timeout on every request, in Open-Meteo's
flatbuffers format: the same float32 values getData decodes through the SDK,
where the JSON format would round them to one decimal. Units are asked for in
canonical form and then checked anyway, from the unit each variable reports -
Open-Meteo silently ignores a parameter it does not know, so asking is not
proof.
"""
from datetime import datetime, timedelta, timezone

import httpx
import numpy as np
from openmeteo_sdk.Unit import Unit
from openmeteo_sdk.Variable import Variable
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse

from core.model import Location
from core.pipeline import SourceResult
from core.units import UnitError, to_canonical
from sources import http
from sources.base import SourceError

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
DEFAULT_TIMEOUT_S = 30.0

# (SDK variable, altitude in metres) -> variable name
SDK_VARIABLES = {
    (Variable.temperature, 2): "temperature_2m",
    (Variable.precipitation, 0): "precipitation",
    (Variable.cloud_cover, 0): "cloud_cover",
    (Variable.wind_speed, 10): "wind_speed_10m",
}

# SDK unit -> core.units spelling. Anything else is refused.
SDK_UNITS = {
    Unit.celsius: "degC",
    Unit.fahrenheit: "degF",
    Unit.kelvin: "K",
    Unit.millimetre: "mm",
    Unit.inch: "inch",
    Unit.centimetre: "cm",
    Unit.percentage: "percent",
    Unit.fraction: "fraction",
    Unit.metre_per_second: "m/s",
    Unit.kilometres_per_hour: "km/h",
    Unit.knots: "kn",
    Unit.miles_per_hour: "mph",
}
_SDK_UNIT_NAMES = {value: name for name, value in vars(Unit).items() if not name.startswith("_")}

# The canonical units, requested outright. Open-Meteo's wind default is km/h.
UNIT_PARAMS = {"temperature_unit": "celsius", "precipitation_unit": "mm", "wind_speed_unit": "ms"}


class OpenMeteoEnsemble:
    kind = "ensemble"
    native_step = timedelta(hours=1)
    variables = frozenset(SDK_VARIABLES.values())
    quantile_levels = None  # members, so any level can be computed

    def __init__(self, model="ecmwf_ifs025", *, forecast_days=15, timeout_s=DEFAULT_TIMEOUT_S,
                 client=None, url=ENSEMBLE_URL):
        """forecast_days: ask for this much. Open-Meteo pads whatever lies past
        the end of the model run with NaN, and the pipeline drops it, so asking
        for the model's full nominal range is safe (ecmwf_ifs025: 15 days).

        client: an httpx.AsyncClient to share connections; this adapter's own
        timeout is applied to each request regardless of the client's.
        """
        self.id = f"open-meteo:{model}"
        self.model = model
        self.max_lead = timedelta(days=forecast_days)
        self.forecast_days = forecast_days
        self.timeout = httpx.Timeout(timeout_s)
        self.url = url
        self._client = client

    def params(self, location, variables):
        return {
            "latitude": location.lat,
            "longitude": location.lon,
            "hourly": ",".join(sorted(variables)),
            "models": self.model,
            # Steps start at local midnight, so the 6-hour grid lands on 00/06/12/18
            # local time. The data itself stays in UTC.
            "timezone": "auto",
            "forecast_days": self.forecast_days,
            **UNIT_PARAMS,
            "format": "flatbuffers",
        }

    async def fetch(self, location, variables):
        variables = set(variables)
        unknown = variables - self.variables
        if unknown:
            raise ValueError(f"{self.id} cannot deliver {', '.join(sorted(unknown))}")
        response = await http.get(self.url, self.params(location, variables), timeout=self.timeout,
                                  what=self.id, client=self._client)
        return decode(response.content, self.id, variables, name=location.name)


def messages(content):
    """Split a flatbuffers response into its length-prefixed messages."""
    pos = 0
    while pos < len(content):
        length = int.from_bytes(content[pos : pos + 4], byteorder="little")
        if length == 0 or pos + 4 + length > len(content):
            raise SourceError(f"truncated flatbuffers response at byte {pos}")
        yield WeatherApiResponse.GetRootAs(content, pos + 4)
        pos += 4 + length


def decode(content, source_id, variables, name=None):
    """A flatbuffers response for one location -> SourceResult in canonical units."""
    responses = list(messages(content))
    if len(responses) != 1:
        raise SourceError(f"expected one location in the response, got {len(responses)}")
    response = responses[0]
    hourly = response.Hourly()
    if hourly is None:
        raise SourceError("the response has no hourly block")
    if hourly.Interval() != 3600:
        raise SourceError(f"expected hourly rows, got a {hourly.Interval()}s interval")

    members = {}
    for i in range(hourly.VariablesLength()):
        series = hourly.Variables(i)
        variable = SDK_VARIABLES.get((series.Variable(), series.Altitude()))
        if variable is None:
            raise SourceError(
                f"unexpected variable {series.Variable()} at {series.Altitude()} m in the response"
            )
        unit = SDK_UNITS.get(series.Unit())
        if unit is None:
            raise SourceError(
                f"{variable} arrived in {_SDK_UNIT_NAMES.get(series.Unit(), series.Unit())}, "
                f"a unit this adapter does not know"
            )
        try:
            values = to_canonical(series.ValuesAsNumpy(), unit, variable)
        except UnitError as exc:
            raise SourceError(str(exc)) from exc
        members.setdefault(variable, {})[series.EnsembleMember()] = values

    missing = variables - set(members)
    if missing:
        raise SourceError(f"the response lacks {', '.join(sorted(missing))}")
    member_ids = {variable: tuple(sorted(by_member)) for variable, by_member in members.items()}
    if len(set(member_ids.values())) != 1:
        sizes = {variable: len(ids) for variable, ids in member_ids.items()}
        raise SourceError(f"variables have different ensemble members: {sizes}")

    elevation = response.Elevation()
    return SourceResult(
        source=source_id,
        kind="ensemble",
        location=Location(
            lat=response.Latitude(),
            lon=response.Longitude(),
            elevation_m=None if np.isnan(elevation) else float(elevation),
            timezone=(response.Timezone() or b"").decode() or None,
            name=name,
        ),
        start=datetime.fromtimestamp(hourly.Time(), tz=timezone.utc),
        native_step_hours=1,
        members={
            variable: np.stack([by_member[m] for m in sorted(by_member)])
            for variable, by_member in members.items()
        },
    )
