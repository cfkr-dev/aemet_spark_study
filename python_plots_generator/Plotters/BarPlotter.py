import os
from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure

from Server.Models.BarModel import BarModel
from .plotter import Plotter


class BarPlotter(Plotter):
    def __init__(self, bar_model: BarModel):
        self.model = bar_model
        self.dataframe = self.load_dataframe(bar_model.src.path)

    def create_plot(self):
        x_col = self.model.src.axis.x.name
        y_col = self.model.src.axis.y.name
        style = self.model.style

        x_vals = self.dataframe[x_col]
        y_vals = self.dataframe[y_col]

        y_min = min(y_vals)
        y_max = max(y_vals)
        y_range_margin = 0.1 * (y_max - y_min + 1e-6)

        fig = go.Figure(data=go.Bar(
            x=x_vals,
            y=y_vals,
            text=[f"{v:.2f}" for v in y_vals],
            textposition='inside',
            marker_color=style.figure.color,
        ))

        fig.update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>" if style.lettering.subtitle else f"{style.lettering.title}",
            xaxis_title=style.lettering.x_label,
            yaxis_title=style.lettering.y_label,
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            margin=dict(
                l=style.margin.left,
                r=style.margin.right,
                t=style.margin.top,
                b=style.margin.bottom
            ),
            xaxis=dict(
                tickangle=0,
                tickfont=dict(size=12),
                automargin=True,
                type='category'
            ),
            yaxis=dict(
                range=[y_max + y_range_margin, y_min - y_range_margin] if style.figure.inverted_y else [
                    y_min - y_range_margin, y_max + y_range_margin]
            )
        )

        return fig

    def save_plot(self, figure: Figure):
        if figure is None:
            return None

        os.makedirs(str(self.model.dest.path), exist_ok=True)

        figure.write_html(str((self.model.dest.path / Path(self.model.dest.filename + ".html")).resolve()))
        if self.model.dest.export_png:
            figure.write_image(str((self.model.dest.path / Path(self.model.dest.filename + ".png")).resolve()))

        return str(self.model.dest.path)
