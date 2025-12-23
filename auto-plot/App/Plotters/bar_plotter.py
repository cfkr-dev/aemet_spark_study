"""Bar plotter implementation using Plotly.

This module provides :class:`BarPlotter` which accepts a
:class:`App.Api.Models.bar_model.BarModel` instance and produces a
horizontal bar chart using Plotly. The generated figure can be exported
via the project's PlotExport storage backend.
"""

from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure

from App.Api.Models.bar_model import BarModel
from App.Plotters.abstract_plotter import Plotter
from App.Utils.Storage.PlotExport.plot_export_storage_backend import PlotExportStorageBackend
from App.Utils.file_utils import get_response_dest_path


class BarPlotter(Plotter):
    """Plotter for horizontal bar charts.

    :param bar_model: Configuration model containing source, destination and style.
    :type bar_model: App.Api.Models.bar_model.BarModel
    """

    def __init__(self, bar_model: BarModel):
        """Create a :class:`BarPlotter` and load the source dataframe.

        :param bar_model: The configuration model for this plotter.
        :type bar_model: BarModel
        """
        self.storage = bar_model.storage
        self.model = bar_model
        self.dataframe = self.load_dataframe(bar_model.src.path, bar_model.storage)

    def _generate_bar_text(self):
        """Build the HTML text blocks displayed inside/outside each bar.

        The method reads the configured ``inside_info`` specification from
        the model's style and formats each row accordingly.

        :returns: A list of HTML strings, one per dataframe row.
        :rtype: list[str]
        """
        style = self.model.style
        bar_infos = []

        for _, row in self.dataframe.iterrows():
            text = ""
            for info_item in style.lettering.inside_info:
                text += info_item.label + ": "
                for build_item in info_item.build:
                    text += build_item.text_before + str(row[build_item.name]) + build_item.text_after

                text += "<br>"

            bar_infos.append(text)

        return bar_infos

    def create_plot(self):
        """Create the horizontal bar Plotly figure based on the model data.

        :returns: A Plotly figure instance containing the bar chart.
        :rtype: plotly.graph_objs.Figure
        """
        x_col = self.model.src.axis.x.name
        y_col = self.model.src.axis.y.name
        style = self.model.style

        categories = self.dataframe[x_col]
        values = self.dataframe[y_col]

        x_min = min(values)
        x_max = max(values)
        spread = x_max - x_min
        total_abs = abs(x_min) + abs(x_max) + 1e-6
        relative_spread = spread / total_abs

        margin = style.figure.range_margin_perc * total_abs

        if relative_spread < style.figure.threshold_limit_max_min:
            raw_range = [x_min - margin, x_max + margin]
        else:
            if x_max <= 0:
                raw_range = [x_min - margin, 0]
            elif x_min >= 0:
                raw_range = [0, x_max + margin]
            else:
                raw_range = [x_min - margin, x_max + margin]

        x_range = raw_range[::-1] if style.figure.inverted_horizontal_axis else raw_range

        texts = self._generate_bar_text()

        threshold = style.figure.threshold_perc_limit_outside_text * (x_range[1] - x_range[0])

        text_positions = []
        for val in values:
            if abs(val) >= threshold:
                text_positions.append('inside')
            else:
                text_positions.append('outside')

        fig = go.Figure(data=go.Bar(
            y=categories,
            x=values,
            orientation='h',
            text=texts,
            textposition=text_positions,
            insidetextanchor='end',
            marker_color=style.figure.color
        ))

        fig.update_layout(
            title=(
                f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>"
                if style.lettering.subtitle else
                f"{style.lettering.title}"
            ),
            xaxis_title=style.lettering.y_label,
            yaxis_title=style.lettering.x_label,
            uniformtext_minsize=12,
            uniformtext_mode='hide',
            margin=dict(
                l=style.margin.left,
                r=style.margin.right,
                t=style.margin.top,
                b=style.margin.bottom
            ),
            xaxis=dict(
                range=x_range,
                tickfont=dict(size=12),
                automargin=True
            ),
            yaxis=dict(
                type='category',
                automargin=True,
                tickfont=dict(size=12),
                autorange='reversed'
            )
        )

        return fig

    def save_plot(self, figure: Figure):
        """Export the figure as HTML (and optionally PNG) using storage backend.

        :param figure: Plotly figure to export.
        :type figure: plotly.graph_objs.Figure
        :returns: Path returned by the storage helper that points to the output.
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
