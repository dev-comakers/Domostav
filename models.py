"""Pydantic data models for the Domostav write-off analysis pipeline."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MaterialCategory(str, Enum):
    PIPE = "PIPE"
    FITTING = "FITTING"
    INSULATION = "INSULATION"
    CONSUMABLE = "CONSUMABLE"
    VALVE = "VALVE"
    OTHER = "OTHER"


class MatchMethod(str, Enum):
    ARTICLE = "ARTICLE"       # exact article code match
    REGEX = "REGEX"           # regex-based diameter/type extraction
    AI = "AI"                 # Claude API matching
    MANUAL = "MANUAL"         # manual override
    UNMATCHED = "UNMATCHED"


class AnomalyStatus(str, Enum):
    OK = "OK"                 # <15% deviation
    WARNING = "WARNING"       # 15-30% deviation
    RED_FLAG = "RED_FLAG"     # >30% or unmatched


class ColumnMapping(BaseModel):
    """Mapping of column letters/indices to semantic roles for an Excel file."""
    row_number: Optional[str] = None
    article: Optional[str] = None
    name: Optional[str] = None
    unit: Optional[str] = None
    quantity: Optional[str] = None
    quantity_accounting: Optional[str] = None
    deviation: Optional[str] = None
    price: Optional[str] = None
    total: Optional[str] = None
    percent_month: Optional[str] = None
    total_month: Optional[str] = None
    header_row: int = 1
    data_start_row: int = 2


class SPPItem(BaseModel):
    """A single line item from the SPP (list of performed works)."""
    row: int
    sheet: str
    name: str
    unit: Optional[str] = None
    quantity: Optional[float] = None
    price_per_unit: Optional[float] = None
    total: Optional[float] = None
    percent_month: Optional[float] = None
    total_month: Optional[float] = None
    # extracted features
    diameter: Optional[int] = None
    category: MaterialCategory = MaterialCategory.OTHER
    material_type: Optional[str] = None  # e.g., "PPR", "PE", "Cu"


class InventoryItem(BaseModel):
    """A single line item from the inventory document."""
    row: int
    number: Optional[str] = None        # sequential number
    article: Optional[str] = None       # article code like STRE020S4
    name: str
    unit: Optional[str] = None
    quantity_fact: Optional[float] = None
    quantity_accounting: Optional[float] = None
    deviation: Optional[float] = None   # fact - accounting
    price: Optional[float] = None
    # extracted features
    diameter: Optional[int] = None
    category: MaterialCategory = MaterialCategory.OTHER
    material_type: Optional[str] = None


class NomenclatureItem(BaseModel):
    """An item from the nomenclature reference list."""
    group: Optional[str] = None         # e.g., "0001 TRUBKY PPR"
    name: str
    unit: Optional[str] = None
    article: Optional[str] = None
    diameter: Optional[int] = None
    category: MaterialCategory = MaterialCategory.OTHER


class MatchResult(BaseModel):
    """Result of matching an inventory item to SPP items."""
    inventory_row: int
    matched_spp_rows: list[int] = Field(default_factory=list)
    match_method: MatchMethod = MatchMethod.UNMATCHED
    confidence: float = 0.0             # 0.0 - 1.0
    match_reason: str = ""


class WriteoffRecommendation(BaseModel):
    """AI recommendation for a single inventory line."""
    inventory_row: int
    inventory_name: str
    article: Optional[str] = None
    expected_writeoff: Optional[float] = None
    actual_deviation: Optional[float] = None
    spp_reference: str = ""             # which SPP items it maps to
    reason: str = ""
    status: AnomalyStatus = AnomalyStatus.RED_FLAG
    match_method: MatchMethod = MatchMethod.UNMATCHED
    deviation_percent: Optional[float] = None
