import os
import plotly.graph_objects as go

from pathlib import Path
from plotly.graph_objs import Figure

from App.Plotters.abstract_plotter import Plotter
from App.Api.Models.pie_model import PieModel
from App.Utils.file_utils import get_response_dest_path


class PiePlotter(Plotter):
    def __init__(self, pie_model: PieModel):
        self.model = pie_model
        self.dataframe = self.load_dataframe(pie_model.src.path)

    def create_plot(self):
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
        if figure is None:
            return None

        os.makedirs(str(self.model.dest.path), exist_ok=True)

        figure.write_html(str((self.model.dest.path / Path(self.model.dest.filename + ".html")).resolve()))
        if self.model.dest.export_png:
            figure.write_image(str((self.model.dest.path / Path(self.model.dest.filename + ".png")).resolve()))

        return get_response_dest_path(self.model.dest.path)
