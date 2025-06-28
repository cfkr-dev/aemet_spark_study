from pathlib import Path

from App.Config.constants import STORAGE_BASE_DIR, OUTPUT_BASE_DIR

def get_src_path(src_path: str):
    return (STORAGE_BASE_DIR / Path(src_path)).resolve()

def get_dest_path(dest_path: str):
    return (OUTPUT_BASE_DIR / Path(dest_path)).resolve()