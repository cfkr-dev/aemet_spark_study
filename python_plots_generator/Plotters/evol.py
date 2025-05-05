import os

import pandas as pd
import plotly.graph_objects as go

import Config.constants as cts
from .plotter import Plotter
from PlotterStyles import LinearPlotterStyle
from Utils import *


class LinearPlotter(Plotter):
    def __init__(self, src_path: str, src_x_col: str, src_y_col: str, src_format_schema: dict, dest_path: str,
                 dest_filename: str, style: LinearPlotterStyle):
        self.src_x_col = src_x_col
        self.src_y_col = src_y_col
        self.dataframe = format_df(self.load_dataframe(src_path), src_format_schema)
        self.style = style
        self.figure = self.create_plot()
        self.path_to_save = cts.SPARK_BASE_DIR + dest_path
        self.dest_filename = dest_filename

    def create_plot(self):
        df = self.dataframe

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df[self.src_x_col],
            y=df[self.src_y_col],
            mode='lines+markers',
            showlegend=self.style.get_show_legend(),
            line=dict(color=self.style.get_figure_color(), width=2),
            marker=dict(size=6)
        ))

        fig.update_layout(
            title=self.style.get_title(),
            xaxis_title=self.style.get_xaxis_label(),
            yaxis_title=self.style.get_yaxis_label(),
            template='plotly_white',
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=-0.2,
                xanchor='center',
                x=0.5
            )
        )

        return fig

    def save_plot(self):
        os.makedirs(self.path_to_save, exist_ok=True)
        self.figure.write_image(self.path_to_save + self.dest_filename + ".png")
        self.figure.write_html(self.path_to_save + self.dest_filename + ".html")

        return self.path_to_save
