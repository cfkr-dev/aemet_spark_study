import pathlib
from pathlib import Path

from App.Config.constants import STORAGE_BASE, OUTPUT_BASE, K_OUTPUT_DIR_NAME


def get_src_path(src_path: str):
    return STORAGE_BASE / Path(src_path)

def get_dest_path(dest_path: str):
    return OUTPUT_BASE / Path(dest_path)

def get_response_dest_path(dest_path: pathlib.Path):
    try:
        parts = dest_path.parts
        index = parts.index(K_OUTPUT_DIR_NAME)
        return Path(*parts[index:]).as_posix()
    except ValueError:
        return None