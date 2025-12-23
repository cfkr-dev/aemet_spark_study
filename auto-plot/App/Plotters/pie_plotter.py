"""Pie chart plotter using Plotly.

This module implements :class:`PiePlotter` which creates a pie chart from
configured lower/upper bound columns and a value column.
"""

from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure

from App.Api.Models.pie_model import PieModel
from App.Plotters.abstract_plotter import Plotter
from App.Utils.Storage.PlotExport.plot_export_storage_backend import PlotExportStorageBackend
from App.Utils.file_utils import get_response_dest_path


class PiePlotter(Plotter):
    """Plotter for categorical pie charts.

    :param pie_model: Model describing source, destination and style for the chart.
    :type pie_model: App.Api.Models.pie_model.PieModel
    """
    def __init__(self, pie_model: PieModel):
        """Initialize and load the source dataframe.

        :param pie_model: Configuration model for pie plots.
        :type pie_model: PieModel
        """
        self.storage = pie_model.storage
        self.model = pie_model
        self.dataframe = self.load_dataframe(pie_model.src.path, pie_model.storage)

    def create_plot(self):
        """Create a Plotly pie chart based on the configuration.

        :returns: A Plotly figure instance containing the pie chart.
        :rtype: plotly.graph_objs.Figure
        """
        lower_bound_col = self.model.src.names.lower_bound
        upper_bound_col = self.model.src.names.upper_bound
        values_col = self.model.src.names.value

        style = self.model.style

        sets = [f"{int(lower_bound)} - {int(upper_bound)}" for index, (lower_bound, upper_bound)
                in enumerate(zip(self.dataframe[lower_bound_col], self.dataframe[upper_bound_col]))]

        return go.Figure(data=[go.Pie(
            labels=sets,
            values=self.dataframe[values_col],
            textinfo='label+percent',
            insidetextorientation='radial',
            showlegend=style.show_legend
        )]).update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>" if style.lettering.subtitle else f"{style.lettering.title}",
            margin=dict(l=style.margin.left, r=style.margin.right, t=style.margin.top, b=style.margin.bottom)
        )

    def save_plot(self, figure: Figure):
        """Export the pie chart figure using the PlotExport backend.

        :param figure: Plotly figure created by :meth:`create_plot`.
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
