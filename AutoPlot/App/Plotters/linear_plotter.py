from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure

from App.Api.Models.linear_model import LinearModel
from App.Plotters.abstract_plotter import Plotter
from App.Utils.Storage.Core.storage import Storage
from App.Utils.Storage.PlotExport.plot_export_storage_backend import PlotExportStorageBackend
from App.Utils.dataframe_formatter import format_df
from App.Utils.file_utils import get_response_dest_path


class LinearPlotter(Plotter):
    def __init__(self, linear_model: LinearModel):
        self.storage = linear_model.storage
        self.model = linear_model
        self.dataframe = format_df(self.load_dataframe(linear_model.src.path, linear_model.storage), {
            linear_model.src.axis.x.name: linear_model.src.axis.x.format
        })

    def create_plot(self):
        x_col = self.model.src.axis.x.name
        y_col = self.model.src.axis.y.name
        style = self.model.style

        x_min = self.dataframe[x_col].min()
        x_max = self.dataframe[x_col].max()

        # Calcular margen
        margin = (x_max - x_min) * 0.1  # 10% de margen
        x_range = [x_min - margin, x_max + margin]

        return go.Figure().add_trace(
            go.Scattergl(
                x=self.dataframe[x_col],
                y=self.dataframe[y_col],
                mode='lines',
                name=style.figure.name,
                showlegend=style.legend.show_legend,
                line=dict(color=style.figure.color, width=1),
            )
        ).update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>" if style.lettering.subtitle else f"{style.lettering.title}",
            xaxis_title=style.lettering.x_label,
            yaxis_title=style.lettering.y_label,
            template='plotly_white',
            hovermode='x unified',
            xaxis_range=x_range,  # Aquí se centra la vista
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=style.legend.y_offset,
                xanchor='center',
                x=0.5
            ),
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
