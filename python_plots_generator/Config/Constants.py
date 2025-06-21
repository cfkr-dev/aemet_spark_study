import os
from pathlib import Path

# ------------------------
# ----   KEY NAMES   ----
# ------------------------

# ENV
K_STORAGE_BASE_DIR = "STORAGE_BASE_DIR"

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
K_MONTHS_SP_ENE = "Ene"
K_MONTHS_SP_FEB = "Feb"
K_MONTHS_SP_MAR = "Mar"
K_MONTHS_SP_ABR = "Abr"
K_MONTHS_SP_MAY = "May"
K_MONTHS_SP_JUN = "Jun"
K_MONTHS_SP_JUL = "Jul"
K_MONTHS_SP_AGO = "Ago"
K_MONTHS_SP_SEP = "Sep"
K_MONTHS_SP_OCT = "Oct"
K_MONTHS_SP_NOV = "Nov"
K_MONTHS_SP_DIC = "Dic"

MONTHS_SP_LIST = [
    K_MONTHS_SP_ENE,
    K_MONTHS_SP_FEB,
    K_MONTHS_SP_MAR,
    K_MONTHS_SP_ABR,
    K_MONTHS_SP_MAY,
    K_MONTHS_SP_JUN,
    K_MONTHS_SP_JUL,
    K_MONTHS_SP_AGO,
    K_MONTHS_SP_SEP,
    K_MONTHS_SP_OCT,
    K_MONTHS_SP_NOV,
    K_MONTHS_SP_DIC
]

# ---------------------
# ----   STORAGE   ----
# ---------------------

STORAGE_BASE_DIR = Path(os.getenv("STORAGE_BASE_DIR")).resolve()
OUTPUT_BASE_DIR = (STORAGE_BASE_DIR / Path("output")).resolve()
