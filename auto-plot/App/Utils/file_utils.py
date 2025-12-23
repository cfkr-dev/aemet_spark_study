"""Utility helpers for resolving source and destination filesystem paths.

The functions in this module centralize the construction of source and
destination :class:`pathlib.Path` objects relative to the configured
storage roots. They are intentionally small and side-effect free.
"""

import pathlib
from pathlib import Path

from App.Config.constants import STORAGE_BASE, OUTPUT_BASE, K_OUTPUT_DIR_NAME


def get_src_path(src_path: str):
    """Resolve a source path string into an absolute storage path.

    If ``src_path`` is absolute it will be converted to a relative path
    component before joining with the configured ``STORAGE_BASE``.

    :param src_path: Source path string from configuration.
    :type src_path: str
    :returns: Resolved :class:`pathlib.Path` under the storage base.
    :rtype: pathlib.Path
    """
    path = Path(src_path)
    if path.is_absolute():
        path = path.relative_to("/")
    return STORAGE_BASE / path


def get_dest_path(dest_path: str):
    """Resolve a destination path string into the output base path.

    If ``dest_path`` is absolute it will be converted to a relative path
    component before joining with the configured ``OUTPUT_BASE``.

    :param dest_path: Destination path string from configuration.
    :type dest_path: str
    :returns: Resolved :class:`pathlib.Path` under the output base.
    :rtype: pathlib.Path
    """
    path = Path(dest_path)
    if path.is_absolute():
        path = path.relative_to("/")
    return OUTPUT_BASE / path


def get_response_dest_path(dest_path: pathlib.Path):
    """Return the response path (path fragment) relative to the output directory.

    The function searches for the configured output directory name inside
    the provided ``dest_path`` and returns the subpath starting at that
    directory in POSIX form. If the output directory name is not present
    this function returns ``None``.

    :param dest_path: Absolute or resolved destination :class:`pathlib.Path`.
    :type dest_path: pathlib.Path
    :returns: POSIX string representing the path fragment from the output dir or ``None``.
    :rtype: str | None
    """
    try:
        parts = dest_path.parts
        index = parts.index(K_OUTPUT_DIR_NAME)
        return Path(*parts[index:]).as_posix()
    except ValueError:
        return None
