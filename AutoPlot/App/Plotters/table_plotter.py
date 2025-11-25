from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure

from App.Api.Models.table_model import TableModel
from App.Plotters.abstract_plotter import Plotter
from App.Utils.Storage.Core.storage import Storage
from App.Utils.Storage.PlotExport.plot_export_storage_backend import PlotExportStorageBackend
from App.Utils.file_utils import get_response_dest_path


class TablePlotter(Plotter):
    def __init__(self, table_model: TableModel):
        self.storage = table_model.storage
        self.model = table_model
        self.dataframe = self.load_dataframe(table_model.src.path, table_model.storage)

    def create_plot(self):

        style = self.model.style

        return go.Figure(data=[go.Table(
            header=dict(
                values=style.lettering.headers,
                align=style.figure.headers.align.value,
                fill_color=style.figure.headers.color,
                font=dict(color='black', size=14)
            ),
            cells=dict(
                values=self.dataframe[self.model.src.col_names].T,
                align=style.figure.cells.align.value,
                fill_color=style.figure.cells.color,
                font=dict(color='black', size=12)
            )
        )]).update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>"
            if style.lettering.subtitle else
            f"{style.lettering.title}",
            margin=dict(l=style.margin.left, r=style.margin.right, t=style.margin.top, b=style.margin.bottom)
        )

    def save_plot(self, figure: Figure):
        if figure is None:
            return None

        PlotExportStorageBackend.export_html(
            str((self.model.dest.path / Path(self.model.dest.filename + ".html")).as_posix()),
            figure,
            self.storage
        )
        if self.model.dest.export_png:
            PlotExportStorageBackend.export_png(
                str((self.model.dest.path / Path(self.model.dest.filename + ".png")).as_posix()),
                figure,
                self.storage
            )

        return get_response_dest_path(self.model.dest.path)
