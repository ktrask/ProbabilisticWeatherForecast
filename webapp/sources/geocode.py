"""Place search for the location box.

Open-Meteo's geocoding API rather than Nominatim, which the legacy app used.
The frontend searches while the user types, and Nominatim's usage policy
rules that out ("you must not implement such a service on the client side
using the API") along with anything above one request per second. Open-Meteo's
service is made for it, comes from the same provider as the forecasts, and
reports each place's time zone and elevation. A different geocoder only has
to offer the same search().
"""
import httpx
from pydantic import BaseModel, ValidationError

from sources import http
from sources.base import SourceError

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
DEFAULT_TIMEOUT_S = 10.0


class Place(BaseModel):
    name: str
    lat: float
    lon: float
    admin1: str | None = None  # state or region
    country: str | None = None
    country_code: str | None = None
    timezone: str | None = None
    elevation_m: float | None = None


class OpenMeteoGeocoder:
    id = "open-meteo:geocoding"

    def __init__(self, *, timeout_s=DEFAULT_TIMEOUT_S, client=None, url=GEOCODING_URL):
        self.timeout = httpx.Timeout(timeout_s)
        self.url = url
        self._client = client

    async def search(self, query, *, count=5, language="en"):
        """Places matching `query`, best first; an empty list if there are none."""
        query = query.strip()
        if len(query) < 2:
            # Open-Meteo answers nothing for a single character either.
            return []
        params = {"name": query, "count": count, "language": language, "format": "json"}
        response = await http.get(self.url, params, timeout=self.timeout, what=self.id, client=self._client)
        try:
            return [
                Place(
                    name=hit["name"],
                    lat=hit["latitude"],
                    lon=hit["longitude"],
                    admin1=hit.get("admin1"),
                    country=hit.get("country"),
                    country_code=hit.get("country_code"),
                    timezone=hit.get("timezone"),
                    elevation_m=hit.get("elevation"),
                )
                for hit in response.json().get("results", [])
            ]
        except (ValueError, KeyError, TypeError, AttributeError, ValidationError) as exc:
            raise SourceError(f"{self.id} sent an answer that is not a list of places: {exc}") from exc
