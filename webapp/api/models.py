"""Response models of the API, apart from the Forecast itself (core.model)."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from sources.geocode import Place


class ProductOut(BaseModel):
    id: str
    label: str
    default: bool
    variants: list[str] = Field(description="What `variant` accepts for this product.")
    variables: list[str]
    schemes: list[str] = Field(description="The VSUP schemes its pictograms are drawn with.")


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
