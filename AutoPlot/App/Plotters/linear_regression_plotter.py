import os
import plotly.graph_objects as go

from pathlib import Path
from plotly.graph_objs import Figure

from App.Plotters.abstract_plotter import Plotter
from App.Api.Models.linear_regression_model import LinearRegressionModel
from App.Utils.dataframe_formatter import format_df
from App.Utils.file_utils import get_response_dest_path


class LinearRegressionParams:
    def __init__(self, df, model: LinearRegressionModel):
        self.slope = float(df[model.src.regression.names.slope].iloc[0])
        self.intercept = float(df[model.src.regression.names.intercept].iloc[0])

class LinearRegressionPlotter(Plotter):
    def __init__(self, linear_regression_model: LinearRegressionModel):
        self.model = linear_regression_model
        self.dataframe = format_df(self.load_dataframe(linear_regression_model.src.main.path), {
            linear_regression_model.src.main.axis.x.name: linear_regression_model.src.main.axis.x.format
        })
        self.regression = LinearRegressionParams(self.load_dataframe(linear_regression_model.src.regression.path), linear_regression_model)

    def create_plot(self):
        x_col = self.model.src.main.axis.x.name
        y_col = self.model.src.main.axis.y.name
        style = self.model.style

        x_min = self.dataframe[x_col].min()
        x_max = self.dataframe[x_col].max()

        # Calcular margen
        margin = (x_max - x_min) * 0.1  # 10% de margen
        x_range = [x_min - margin, x_max + margin]

        regression_function = lambda x: self.regression.slope * x.year + self.regression.intercept

        return (go.Figure()
        .add_trace(
            go.Scattergl(
                x=self.dataframe[x_col],
                y=self.dataframe[y_col],
                mode='lines',
                name=style.figure_1.name,
                showlegend=style.legend.show_legend,
                line=dict(color=style.figure_1.color, width=1),
                yaxis='y1'
            )
        )
        .add_trace(
            go.Scattergl(
                x=[x_min, x_max],
                y=[regression_function(x_min), regression_function(x_max)],
                mode='lines',
                name=style.figure_2.name,
                showlegend=style.legend.show_legend,
                line=dict(color=style.figure_2.color, width=1),
                yaxis='y2'
            )
        )
        .update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>" if style.lettering.subtitle else f"{style.lettering.title}",
            xaxis_title=style.lettering.x_label,
            yaxis=dict(
                title=dict(text=style.lettering.y_1_label),
                side='left'
            ),
            yaxis2=dict(
                title=dict(text=style.lettering.y_2_label),
                overlaying='y',
                side='right',
                showgrid=False,
                zeroline=False,
            ),
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
        ))

    def save_plot(self, figure: Figure):
        if figure is None:
            return None

        os.makedirs(str(self.model.dest.path), exist_ok=True)

        figure.write_html(str((self.model.dest.path / Path(self.model.dest.filename + ".html")).resolve()))
        if self.model.dest.export_png:
            figure.write_image(str((self.model.dest.path / Path(self.model.dest.filename + ".png")).resolve()))

        return get_response_dest_path(self.model.dest.path)
