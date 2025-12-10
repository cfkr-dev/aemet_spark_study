import math
from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure
from plotly.subplots import make_subplots

from App.Api.Models.climograph_model import ClimographModel
from App.Config.constants import MONTHS_SP_LIST
from App.Plotters.abstract_plotter import Plotter
from App.Utils.Storage.Core.storage import Storage
from App.Utils.Storage.PlotExport.plot_export_storage_backend import PlotExportStorageBackend
from App.Utils.file_utils import get_response_dest_path


class ClimographPlotter(Plotter):
    def __init__(self, climograph_model: ClimographModel):
        self.storage = climograph_model.storage
        self.model = climograph_model
        self.dataframe = self.load_dataframe(climograph_model.src.path, climograph_model.storage)

    def create_plot(self):
        x_col = self.model.src.axis.x.name
        y_temp_col = self.model.src.axis.y_temp.name
        y_prec_col = self.model.src.axis.y_prec.name
        style = self.model.style

        factor = 2
        visual_max = math.ceil(
            max(
                self.dataframe[y_prec_col].max() + 5,
                ((self.dataframe[y_temp_col].max() + 5) * factor)
            ) / 10
        ) * 10

        dtick_prec = visual_max / 6
        dtick_temp = dtick_prec / (factor * 2)

        return make_subplots(
            specs=[[{"secondary_y": True}]]
        ).add_trace(
            go.Bar(
                x=self.dataframe[x_col],
                y=self.dataframe[y_prec_col],
                name=style.figure_prec.name,
                marker=dict(color=style.figure_prec.color)
            ),
            secondary_y=False
        ).add_trace(
            go.Scatter(
                x=self.dataframe[x_col],
                y=self.dataframe[y_temp_col],
                name=style.figure_temp.name,
                mode="lines+markers",
                line=dict(color=style.figure_temp.color, width=2),
                marker=dict(size=6)
            ),
            secondary_y=True
        ).update_layout(
            xaxis=dict(
                tickmode="array",
                tickvals=self.dataframe[x_col],
                ticktext=MONTHS_SP_LIST,
                title=style.lettering.x_label
            )
        ).update_yaxes(
            title_text=style.lettering.y_prec_label,
            range=[
                (self.dataframe[y_temp_col].min() - 5) * factor if self.dataframe[y_temp_col].min() < 0 else 0,
                visual_max
            ],
            dtick=dtick_prec,
            showgrid=True,
            secondary_y=False,
            tickmode="linear"
        ).update_yaxes(
            title_text=style.lettering.y_temp_label,
            range=[
                self.dataframe[y_temp_col].min() - 5 if self.dataframe[y_temp_col].min() < 0 else 0,
                visual_max / factor
            ],
            dtick=dtick_temp,
            showgrid=False,
            secondary_y=True,
            tickmode="linear"
        ).update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>" if style.lettering.subtitle else f"{style.lettering.title}",
            showlegend=style.legend.show_legend,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=style.legend.y_offset,
                xanchor="center",
                x=0.5
            ),
            margin=dict(
                l=style.margin.left,
                r=style.margin.right,
                t=style.margin.top,
                b=style.margin.bottom
            )
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
