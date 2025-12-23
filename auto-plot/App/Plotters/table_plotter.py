"""Table plotter that renders dataframe rows as an HTML table using Plotly.

This module implements :class:`TablePlotter` which reads column headers
and visual definitions from the model and produces a Plotly table
suitable for export.
"""

from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure

from App.Api.Models.table_model import TableModel
from App.Plotters.abstract_plotter import Plotter
from App.Utils.Storage.PlotExport.plot_export_storage_backend import PlotExportStorageBackend
from App.Utils.file_utils import get_response_dest_path


class TablePlotter(Plotter):
    """Plotter for tabular data export.

    :param table_model: Configuration model including source, destination and style.
    :type table_model: App.Api.Models.table_model.TableModel
    """

    def __init__(self, table_model: TableModel):
        """Initialize and load the source dataframe.

        :param table_model: The configuration model for the table plot.
        :type table_model: TableModel
        """
        self.storage = table_model.storage
        self.model = table_model
        self.dataframe = self.load_dataframe(table_model.src.path, table_model.storage)

    def create_plot(self):

        """Create a Plotly table figure using the model's figure definitions.

        The method reads header/cell visual definitions and the configured
        column order from the model and constructs a Plotly Table trace.

        :returns: A Plotly Table figure.
        :rtype: plotly.graph_objs.Figure
        """

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
        """Export the table figure to HTML (and optionally PNG).

        :param figure: Plotly figure produced by :meth:`create_plot`.
        :returns: Resolved destination path returned by the storage helper.
        """
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
