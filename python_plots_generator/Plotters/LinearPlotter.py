import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go

import Config.constants as cts
from .plotter import Plotter
from PlotterStyles import LinearPlotterStyle
from Utils import *

# todo añadir centralizar
# todo añadir en evolucion la recta de regresion
class LinearPlotter(Plotter):
    def __init__(self, src_path: str, src_x_col: str, src_y_col: str, src_format_schema: dict, dest_path: str,
                 plot_filename: str, style: LinearPlotterStyle):
        self.src_x_col = src_x_col
        self.src_y_col = src_y_col
        self.dataframe = format_df(self.load_dataframe(src_path), src_format_schema)
        self.style = style
        self.figure = self.create_plot()
        self.path_to_save = cts.SPARK_BASE_DIR + dest_path
        self.dest_filename = plot_filename

    def create_plot(self):
        x_min = datetime(2024, 1, 1)

        # Fecha final basada en los datos
        x_max = self.dataframe[self.src_x_col].max()

        # Calcular margen
        margin = (x_max - x_min) * 0.1  # 10% de margen
        x_range = [x_min - margin, x_max + margin]

        return go.Figure().add_trace(
            go.Scattergl(
                x=self.dataframe[self.src_x_col],
                y=self.dataframe[self.src_y_col],
                mode='lines',
                name=self.style.get_figure_name(),
                showlegend=self.style.get_show_legend(),
                line=dict(color=self.style.get_figure_color(), width=1),
            )
        ).update_layout(
            title=self.style.get_title(),
            xaxis_title=self.style.get_xaxis_label(),
            yaxis_title=self.style.get_yaxis_label(),
            template='plotly_white',
            hovermode='x unified',
            xaxis_range=x_range,  # Aquí se centra la vista
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=-0.3,
                xanchor='center',
                x=0.5
            )
        )

    def save_plot(self):
        os.makedirs(self.path_to_save, exist_ok=True)
        self.figure.write_image(self.path_to_save + self.dest_filename + ".png")
        self.figure.write_html(self.path_to_save + self.dest_filename + ".html")

        return self.path_to_save
