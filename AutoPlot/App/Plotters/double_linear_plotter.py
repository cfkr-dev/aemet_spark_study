import os
import plotly.graph_objects as go

from pathlib import Path
from plotly.graph_objs import Figure
from plotly.subplots import make_subplots

from App.Plotters.abstract_plotter import Plotter
from App.Api.Models.double_linear_model import DoubleLinearModel
from App.Utils.dataframe_formatter import format_df
from App.Utils.file_utils import get_response_dest_path


class DoubleLinearPlotter(Plotter):
    def __init__(self, double_linear_model: DoubleLinearModel):
        self.model = double_linear_model
        self.dataframe = format_df(self.load_dataframe(double_linear_model.src.path), {
            double_linear_model.src.axis.x.name: double_linear_model.src.axis.x.format
        })

    def create_plot(self):
        x_col = self.model.src.axis.x.name
        y_1_col = self.model.src.axis.y_1.name
        y_2_col = self.model.src.axis.y_2.name
        style = self.model.style

        x_min = self.dataframe[x_col].min()
        x_max = self.dataframe[x_col].max()

        margin = (x_max - x_min) * 0.1
        x_range = [x_min - margin, x_max + margin]

        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            row_heights=[0.5, 0.5],  # Puedes ajustar el espacio visual
            vertical_spacing=0.07
        )

        fig.add_trace(
            go.Scattergl(
                x=self.dataframe[x_col],
                y=self.dataframe[y_1_col],
                mode='lines',
                name=style.figure_1.name,
                showlegend=style.legend.show_legend,
                line=dict(color=style.figure_1.color, width=1),
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scattergl(
                x=self.dataframe[x_col],
                y=self.dataframe[y_2_col],
                mode='lines',
                name=style.figure_2.name,
                showlegend=style.legend.show_legend,
                line=dict(color=style.figure_2.color, width=1),
            ),
            row=2, col=1
        )

        fig.update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>" if style.lettering.subtitle else f"{style.lettering.title}",
            xaxis=dict(title=style.lettering.x_label, range=x_range),
            yaxis=dict(title=style.lettering.y_1_label),
            yaxis2=dict(title=style.lettering.y_2_label),
            template='plotly_white',
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=style.legend.y_offset,
                xanchor='center',
                x=0.5
            ),
            margin=dict(
                l=style.margin.left,
                r=style.margin.right,
                t=style.margin.top,
                b=style.margin.bottom
            ),
            height=900
        )

        return fig

    def save_plot(self, figure: Figure):
        if figure is None:
            return None

        os.makedirs(str(self.model.dest.path), exist_ok=True)

        figure.write_html(str((self.model.dest.path / Path(self.model.dest.filename + ".html")).resolve()))
        if self.model.dest.export_png:
            figure.write_image(str((self.model.dest.path / Path(self.model.dest.filename + ".png")).resolve()))

        return get_response_dest_path(self.model.dest.path)
