"""Project-wide constants and derived storage paths.

This module centralizes configuration constants used across the application.
It exposes environment variable keys, default names for resources and
constants used by plotting routines (for example, heat-map bounding
coordinates and localized month names). Values here are plain constants
and resolved paths; no runtime logic is performed other than resolving
paths from environment variables where appropriate.

All names are English and documented in reST so they are easy to include
in generated Sphinx documentation.
"""

import os

from pathlib import Path

# ------------------------
# ----   KEY NAMES   ----
# ------------------------

# ENV
K_AWS_S3_ENDPOINT = "AWS_S3_ENDPOINT"
K_STORAGE_PREFIX = "STORAGE_PREFIX"
K_STORAGE_BASE = "STORAGE_BASE"

# STORAGE
K_RESOURCES_COUNTRIES_SHAPEFILE_DIR = "../Resources/countries_shapes"
K_OUTPUT_DIR_NAME = "output"

# FORMATTERS
K_FORMATTER_TIMESTAMP = "timestamp"
K_FORMATTER_TIMESTAMP_YEAR = "timestamp_year"

FORMATTERS_LIST = [
    K_FORMATTER_TIMESTAMP,
    K_FORMATTER_TIMESTAMP_YEAR
]

# TABLE ALIGNS
K_TABLE_ALIGNS_LEFT = "left"
K_TABLE_ALIGNS_CENTER = "center"
K_TABLE_ALIGNS_RIGHT = "right"

TABLE_ALIGNS_LIST = [
    K_TABLE_ALIGNS_LEFT,
    K_TABLE_ALIGNS_CENTER,
    K_TABLE_ALIGNS_RIGHT
]

# MONTHS SPANISH
K_MONTHS_EN_ENE = "Jan"
K_MONTHS_EN_FEB = "Feb"
K_MONTHS_EN_MAR = "Mar"
K_MONTHS_EN_ABR = "Apr"
K_MONTHS_EN_MAY = "May"
K_MONTHS_EN_JUN = "Jun"
K_MONTHS_EN_JUL = "Jul"
K_MONTHS_EN_AGO = "Aug"
K_MONTHS_EN_SEP = "Sep"
K_MONTHS_EN_OCT = "Oct"
K_MONTHS_EN_NOV = "Nov"
K_MONTHS_EN_DIC = "Dic"

MONTHS_EN_LIST = [
    K_MONTHS_EN_ENE,
    K_MONTHS_EN_FEB,
    K_MONTHS_EN_MAR,
    K_MONTHS_EN_ABR,
    K_MONTHS_EN_MAY,
    K_MONTHS_EN_JUN,
    K_MONTHS_EN_JUL,
    K_MONTHS_EN_AGO,
    K_MONTHS_EN_SEP,
    K_MONTHS_EN_OCT,
    K_MONTHS_EN_NOV,
    K_MONTHS_EN_DIC
]

# SPAIN GEOGRAPHIC LOCATIONS
K_SPAIN_GEOGRAPHIC_LOCATIONS_CONTINENTAL = "continental"
K_SPAIN_GEOGRAPHIC_LOCATIONS_CANARY_ISLANDS = "canary_islands"

SPAIN_GEOGRAPHIC_LOCATIONS_LIST = [
    K_SPAIN_GEOGRAPHIC_LOCATIONS_CONTINENTAL,
    K_SPAIN_GEOGRAPHIC_LOCATIONS_CANARY_ISLANDS
]

# ---------------------
# ----   STORAGE   ----
# ---------------------

# Values below are resolved at import time from environment variables.
# They may be ``None`` if the corresponding environment variables are
# not set; callers should handle that case.
STORAGE_PREFIX = os.getenv(K_STORAGE_PREFIX)
STORAGE_BASE = Path(os.getenv(K_STORAGE_BASE))
OUTPUT_BASE = (STORAGE_BASE / Path(K_OUTPUT_DIR_NAME))

# Resolved path for the countries shapefile resources. This is resolved
# relative to the repository layout and returned as an absolute Path.
STORAGE_RESOURCES_COUNTRIES_SHAPEFILE = Path(K_RESOURCES_COUNTRIES_SHAPEFILE_DIR).resolve()

# --------------------
# ----   AWS S3   ----
# --------------------

AWS_S3_ENDPOINT = os.getenv(K_AWS_S3_ENDPOINT)

# ----------------------------------------
# ----   HEAT MAP CHART COORDINATES   ----
# ----------------------------------------

# Continental Spain bounding box used by heat-map plotters. Coordinates
# are expressed as (longitude, latitude) tuples for lower and upper
# bounds respectively.
LONG_LOWER_BOUND_SPAIN_CONTINENTAL, LONG_UPPER_BOUND_SPAIN_CONTINENTAL = (-9.5, 4.5)
LAT_LOWER_BOUND_SPAIN_CONTINENTAL, LAT_UPPER_BOUND_SPAIN_CONTINENTAL = (35.0, 44.0)

# Canary Islands bounding box (smaller region) used when plotting the
# Canaries heat map.
LONG_LOWER_BOUND_SPAIN_CANARY_ISLAND, LONG_UPPER_BOUND_SPAIN_CANARY_ISLAND = (-18.25, -13.25)
LAT_LOWER_BOUND_SPAIN_CANARY_ISLAND, LAT_UPPER_BOUND_SPAIN_CANARY_ISLAND = (27.5, 29.5)