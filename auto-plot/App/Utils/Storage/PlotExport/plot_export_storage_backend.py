import tempfile
import uuid
from pathlib import Path

import plotly.graph_objects as go

from App.Utils.Storage.Core.storage import Storage


class PlotExportStorageBackend:

    TEMP_DIR = Path(tempfile.gettempdir())

    @staticmethod
    def export_html(path: str, fig: go.Figure, storage: Storage):
        html_temp = Path(PlotExportStorageBackend.TEMP_DIR / f"plotexport-{uuid.uuid4()}.html")
        fig.write_html(str(html_temp))
        storage.write(path, html_temp)

        return path

    @staticmethod
    def export_png(path: str, fig: go.Figure, storage: Storage, width: int = 1280, height: int = 720):
        png_temp = Path(PlotExportStorageBackend.TEMP_DIR / f"plotexport-{uuid.uuid4()}.png")
        fig.write_image(str(png_temp), width=width, height=height)
        storage.write(path, png_temp)

        return path