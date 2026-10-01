"""Response models of the API. The forecast is core.model's Forecast, plus which
product drew it."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from core.model import Forecast
from sources.geocode import Place


class AreaOut(BaseModel):
    south: float
    north: float
    west: float
    east: float


class ProductOut(BaseModel):
    id: str
    label: str
    default: bool
    variants: list[str] = Field(description="What `variant` accepts for this product.")
    variables: list[str]
    schemes: list[str] = Field(description="The VSUP schemes its pictograms are drawn with.")
    members: int = Field(description="Ensemble members the model runs.")
    grid_km: float = Field(description="Grid spacing of the model, in km.")
    horizon_days: float = Field(description="About how far ahead a run reaches, in days.")
    automatic: bool = Field(description="Whether the automatic choice may take it.")
    steps: list[int] = Field(description="The step widths in hours it can be drawn in, the first the default.")
    area: AreaOut | None = Field(
        description="For a regional model, the box around its domain: outside it there is no "
                    "forecast, inside it there usually is (rotated grids fill it only partly). "
                    "null: the whole globe."
    )


class ForecastOut(Forecast):
    product: str = Field(description="The product drawn: the one asked for, or the automatic choice.")
    automatic: bool = Field(description="Whether the product was chosen automatically.")


class ProductsOut(BaseModel):
    products: list[ProductOut]


class OutcomeOut(BaseModel):
    model_config = ConfigDict(validate_by_name=True, serialize_by_alias=True)

    pictogram: str = Field(description="Relative to `pictogram_base`.")
    level: int
    class_: str = Field(alias="class")
    condition: str = Field(description="When a rules scheme picks it, as written in the config, in `declared_unit`.")


class ClassOut(BaseModel):
    id: str
    below: float | None = Field(description="Upper bound in `unit`, exclusive; none for the last class.")


class LevelOut(BaseModel):
    level: int
    interval: list[str] | None = Field(
        description="The two quantiles that must fall into one class or group for this level, "
        "e.g. ['p17', 'p83'] - the middle 66 % of the members. None: the level that always applies."
    )


class SchemeOut(BaseModel):
    name: str
    description: str | None
    mode: Literal["rules", "tree"]
    variable: str
    unit: str = Field(description="The variable's canonical unit, which `classes` are given in.")
    declared_unit: str = Field(description="The unit the config was written in.")
    window_hours: int | None
    outcomes: list[OutcomeOut] = Field(description="Every pictogram the scheme can choose, in order.")
    classes: list[ClassOut] | None = Field(description="Tree schemes: the intensity classes.")
    levels: list[LevelOut] | None = Field(
        None, description="Tree schemes: what each certainty level requires, most certain first."
    )


class SchemesOut(BaseModel):
    version: str = Field(description="Changes whenever the config or one of its pictograms does.")
    pictogram_base: str = Field(description="URL prefix for pictogram paths; cacheable for as long as `version` holds.")
    quantiles: list[int]
    schemes: list[SchemeOut]


class GeocodeOut(BaseModel):
    query: str
    results: list[Place]


class CacheStats(BaseModel):
    entries: int
    hits: int
    misses: int


class HealthOut(BaseModel):
    status: Literal["ok", "degraded"]
    vsup_version: str
    products: list[str]
    forecast_cache: CacheStats
    upstream: dict[str, str] | None = Field(None, description="With deep=true: each source's state.")


class ErrorOut(BaseModel):
    detail: str
