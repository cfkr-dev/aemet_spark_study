# --------------------
# ----   NAMING   ----
# --------------------

MONTHS_SP = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre"
]

#region Storage
# Storage
STORAGE_BASE_DIR = "E:/Escritorio/UNIVERSIDAD/TFG/TFG GII - Spark Big Data Study/data/"
# -- Spark
SPARK_BASE_DIR = STORAGE_BASE_DIR + "spark"
# -- Python plots
PY_PLOTS_BASE_DIR = STORAGE_BASE_DIR + "python_plots/"
# ---- Climograph
CLIMOGRAPH_BASE_DIR = PY_PLOTS_BASE_DIR + "climograph/"
# ------ Arid climates
ARID_BASE_DIR = PY_PLOTS_BASE_DIR + "arid_climates/"
# ------ Warm climates
WARM_BASE_DIR = PY_PLOTS_BASE_DIR + "warm_climates/"
# ------ Cold climates
COLD_BASE_DIR = PY_PLOTS_BASE_DIR + "cold_climates/"
#endregion









#region Naming
# -- Climograph
# ---- Data
TEMP_AND_PREC = "temp_and_prec"
STATION = "station"
# ---- Locations
PENINSULA_LAND_NAME = "peninsula"
CANARY_ISLANDS = "canary_islands"
BALEAR_ISLANDS = "balear_islands"
# ---- Climate groups
ARID_CLIMATE = "arid"
WARM_CLIMATE = "warm"
COLD_CLIMATE = "cold"
# ------ Arid climates
BSH_NAME = "BSh"
BSK_NAME = "BSK"
BWH_NAME = "BWh"
BWK_NAME = "BWk"
# ------ Warm climates
CFA_NAME = "BSh"
CFB_NAME = "BSK"
CSA_NAME = "BWh"
CSB_NAME = "BWk"
# ------ Cold climates
DFB_NAME = "BSh"
DFC_NAME = "BSK"
DSB_NAME = "BWh"
#endregion

#region Storage
# Storage
STORAGE_BASE_DIR = "E:/Escritorio/UNIVERSIDAD/TFG/TFG GII - Spark Big Data Study/data/"
# -- Spark
PY_PLOTS_BASE_DIR = STORAGE_BASE_DIR + "python_plots/"
# ---- Climograph
CLIMOGRAPH_BASE_DIR = PY_PLOTS_BASE_DIR + "climograph/"
# ------ Arid climates
ARID_BASE_DIR = PY_PLOTS_BASE_DIR + "arid_climates/"
# ------ Warm climates
WARM_BASE_DIR = PY_PLOTS_BASE_DIR + "warm_climates/"
# ------ Cold climates
COLD_BASE_DIR = PY_PLOTS_BASE_DIR + "cold_climates/"
#endregion

# ARID_DIRS = {
#     BSH_NAME: ARID_BASE_DIR + BSH_NAME + "/",
#     BSK_NAME: ARID_BASE_DIR + BSK_NAME + "/",
#     BWH_NAME: ARID_BASE_DIR + BWH_NAME + "/",
#     BWK_NAME: ARID_BASE_DIR + BWK_NAME + "/",
# }
#
# BSH_DIRS = {
#     PENINSULA_LAND_NAME: ARID_DIRS.get(BSH_NAME) + PENINSULA_LAND_NAME + "/",
#     CANARY_ISLANDS: ARID_DIRS.get(BSH_NAME) + CANARY_ISLANDS + "/",
#     BALEAR_ISLANDS: ARID_DIRS.get(BSH_NAME) + BALEAR_ISLANDS + "/",
# }
#
# BSK_DIRS = {
#     PENINSULA_LAND_NAME: ARID_DIRS.get(BSK_NAME) + PENINSULA_LAND_NAME + "/",
#     CANARY_ISLANDS: ARID_DIRS.get(BSK_NAME) + CANARY_ISLANDS + "/",
#     BALEAR_ISLANDS: ARID_DIRS.get(BSK_NAME) + BALEAR_ISLANDS + "/",
# }
#
# BWH_DIRS = {
#     PENINSULA_LAND_NAME: ARID_DIRS.get(BWH_NAME) + PENINSULA_LAND_NAME + "/",
#     CANARY_ISLANDS: ARID_DIRS.get(BWH_NAME) + CANARY_ISLANDS + "/",
# }
#
# BWK_DIRS = {
#     PENINSULA_LAND_NAME: ARID_DIRS.get(BWK_NAME) + PENINSULA_LAND_NAME + "/",
#     CANARY_ISLANDS: ARID_DIRS.get(BWK_NAME) + CANARY_ISLANDS + "/",
# }