"""Configuration and settings for the Domostav AI pipeline."""

from __future__ import annotations

import os
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"

# LLM provider settings (OpenAI primary, Claude fallback)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.4")
OPENAI_FALLBACK_MODEL = os.environ.get("OPENAI_FALLBACK_MODEL", "gpt-4.1-mini")
APP_SECRET_KEY = os.environ.get("APP_SECRET_KEY", "").strip()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "openai").strip().lower()
MAX_TOKENS = 4096
BATCH_SIZE = 25  # items per API call for matching

# Tolerances
TOLERANCE_OK = 0.15        # <15% deviation = OK
TOLERANCE_WARNING = 0.30   # 15-30% = WARNING, >30% = RED FLAG

# Waste percentages
PIPE_WASTE_PERCENT = 10
INSULATION_WASTE_PERCENT = 5
FITTING_PIPE_COST_RATIO = 0.5  # fittings cost ≈ pipes cost

# Parser defaults (Chirana)
DEFAULT_INVENTORY_HEADER_ROW = 11
DEFAULT_INVENTORY_DATA_START = 12
DEFAULT_SPP_HEADER_ROW = 5
DEFAULT_SPP_DATA_START = 6

# Output
OUTPUT_DIR = PROJECT_ROOT / "output_files"
