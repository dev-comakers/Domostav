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
    OUT_OF_SCOPE = "OUT_OF_SCOPE"  # not relevant to active SPP month


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
    sheet_name: Optional[str] = None
    header_row: int = 1
    data_start_row: int = 2


class SPPItem(BaseModel):
    """A single line item from the SPP (list of performed works)."""
    row: int
    source_row: int
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


class SPPCoverageRec(BaseModel):
    """Coverage result for a single active-month SPP row.

    Primary unit of analysis in the SPP-centric review:
    each active SPP row is checked for whether inventory covers it.
    """
    spp_row: int                          # internal row id
    spp_source_row: int                   # row number in the source Excel
    spp_sheet: str
    spp_name: str
    spp_unit: Optional[str] = None
    spp_qty_month: Optional[float] = None  # expected quantity for this month
    spp_total_month: Optional[float] = None

    # Inventory coverage
    covered_inv_rows: list[int] = Field(default_factory=list)
    covered_inv_names: list[str] = Field(default_factory=list)
    total_inv_deviation: float = 0.0      # sum of |deviation| from matched inventory

    # Result
    delta: Optional[float] = None         # spp_qty_month - total_inv_deviation
    deviation_percent: Optional[float] = None
    status: AnomalyStatus = AnomalyStatus.RED_FLAG
    reason: str = ""
