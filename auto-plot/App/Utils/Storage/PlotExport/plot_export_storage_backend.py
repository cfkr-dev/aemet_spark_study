"""Helpers to export Plotly figures to storage backends.

This module provides :class:`PlotExportStorageBackend` with convenience
methods to export Plotly figures to HTML and PNG files in a temporary
location and then write them to the configured storage backend.

All functions are static and work with the project's abstract
:class:`App.Utils.Storage.Core.storage.Storage` wrapper.
"""

import tempfile
import uuid
from pathlib import Path

import plotly.graph_objects as go

from App.Utils.Storage.Core.storage import Storage


class PlotExportStorageBackend:
    """Utility class that writes Plotly figures to storage.

    The implementation writes temporary files to the system temp
    directory and delegates the final write operation to the provided
    ``Storage`` instance.
    """

    TEMP_DIR = Path(tempfile.gettempdir())

    @staticmethod
    def export_html(path: str, fig: go.Figure, storage: Storage):
        """Export the given Plotly ``fig`` as an HTML file and write to storage.

        :param path: Destination path (logical, passed to :class:`Storage.write`).
        :type path: str
        :param fig: Plotly figure to export.
        :type fig: plotly.graph_objects.Figure
        :param storage: Storage backend used to persist the temporary file.
        :type storage: App.Utils.Storage.Core.storage.Storage
        :returns: The destination path passed by the caller.
        :rtype: str
        """
        html_temp = Path(PlotExportStorageBackend.TEMP_DIR / f"plotexport-{uuid.uuid4()}.html")
        fig.write_html(str(html_temp))
        storage.write(path, html_temp)

        return path

    @staticmethod
    def export_png(path: str, fig: go.Figure, storage: Storage, width: int = 1280, height: int = 720):
        """Export the given Plotly ``fig`` as a PNG image and write to storage.

        :param path: Destination path (logical, passed to :class:`Storage.write`).
        :type path: str
        :param fig: Plotly figure to export.
        :type fig: plotly.graph_objects.Figure
        :param storage: Storage backend used to persist the temporary file.
        :type storage: App.Utils.Storage.Core.storage.Storage
        :param width: Output image width in pixels (defaults to 1280).
        :type width: int
        :param height: Output image height in pixels (defaults to 720).
        :type height: int
        :returns: The destination path passed by the caller.
        :rtype: str
        """
        png_temp = Path(PlotExportStorageBackend.TEMP_DIR / f"plotexport-{uuid.uuid4()}.png")
        fig.write_image(str(png_temp), width=width, height=height)
        storage.write(path, png_temp)

        return path