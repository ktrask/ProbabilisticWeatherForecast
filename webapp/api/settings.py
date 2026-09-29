"""Settings, from the environment."""
import os
from dataclasses import dataclass, fields
from pathlib import Path

from sources.config import DEFAULT_SOURCES
from vsup.config import DEFAULT_CONFIG as DEFAULT_VSUP


@dataclass(frozen=True)
class Settings:
    vsup_config: Path = DEFAULT_VSUP
    sources_config: Path = DEFAULT_SOURCES
    forecast_cache_ttl_s: float = 3600
    forecast_cache_size: int = 256
    geocode_cache_ttl_s: float = 86400
    geocode_cache_size: int = 1024
    geocoder_timeout_s: float = 10
    # Where /api/health?deep=1 asks each source for a forecast. Braunschweig,
    # which the offline fixtures also cover.
    probe_lat: float = 52.26
    probe_lon: float = 10.52

    @classmethod
    def from_env(cls, env=None):
        """VSUP_CONFIG, SOURCES_CONFIG, FORECAST_CACHE_TTL_S, ... - the field
        names in upper case. Unset means the default."""
        env = os.environ if env is None else env
        values = {}
        for field in fields(cls):
            raw = env.get(field.name.upper())
            if raw is None:
                continue
            kind = type(field.default)
            try:
                values[field.name] = Path(raw) if kind is type(DEFAULT_VSUP) else kind(raw)
            except ValueError:
                raise ValueError(f"{field.name.upper()}={raw!r} is not a valid {kind.__name__}") from None
        return cls(**values)
