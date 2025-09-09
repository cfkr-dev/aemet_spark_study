import pathlib
from pathlib import Path

from App.Config.constants import STORAGE_BASE_DIR, OUTPUT_BASE_DIR, K_OUTPUT_DIR_NAME

def get_src_path(src_path: str):
    return (STORAGE_BASE_DIR / Path(src_path)).resolve()

def get_dest_path(dest_path: str):
    return (OUTPUT_BASE_DIR / Path(dest_path)).resolve()

def get_response_dest_path(dest_path: pathlib.Path):
    try:
        parts = dest_path.parts
        index = parts.index(K_OUTPUT_DIR_NAME)
        return str(Path(*parts[index:]))
    except ValueError:
        return None