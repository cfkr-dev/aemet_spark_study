import os
from pathlib import Path

import plotly.graph_objects as go
from plotly.graph_objs import Figure

from app.Server.Models.BarModel import BarModel
from .AbstractPlotter import Plotter


class BarPlotter(Plotter):
    def __init__(self, bar_model: BarModel):
        self.model = bar_model
        self.dataframe = self.load_dataframe(bar_model.src.path)

    def _generate_bar_text(self):
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
        x_col = self.model.src.axis.x.name  # categorías
        y_col = self.model.src.axis.y.name  # valores
        style = self.model.style

        categories = self.dataframe[x_col]  # eje y (categorías)
        values = self.dataframe[y_col]  # eje x (valores)

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

        # Generar texto por barra
        texts = self._generate_bar_text()

        # Umbral para decidir si cabe el texto dentro
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
                autorange='reversed'  # Siempre invertido verticalmente
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
