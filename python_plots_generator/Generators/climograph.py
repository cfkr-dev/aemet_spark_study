import math
import os

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import Config.constants as cts
from Generators.plotter import Plotter


class ClimographPlotter(Plotter):
    def __init__(self, temp_and_prec_path: str, station_path: str, climate_group, climate, location):
        self.plotter_name = "climograph"
        self.climate_group = climate_group
        self.climate = climate
        self.location = location
        self.dataframe_temp_and_prec = self.load_dataframe(temp_and_prec_path)
        self.dataframe_station = self.load_dataframe(station_path)
        self.figure = self.create_plot()
        self.path_to_save = cts.PY_PLOTS_BASE_DIR + self.plotter_name + "/" + self.climate_group + "/" + self.climate + "/" + self.location + "/"

    def create_plot(self):
        df_temp_and_prec = self.dataframe_temp_and_prec
        df_station = self.dataframe_station

        factor = 2
        visual_max = math.ceil(
            max(
                self.dataframe_temp_and_prec["total_prec"].max() + 5,
                ((self.dataframe_temp_and_prec["avg_tmed"].max() + 5) * factor)
            ) / 10
        ) * 10

        dtick_prec = visual_max / 6
        dtick_temp = dtick_prec / (factor * 2)

        return make_subplots(
            specs=[[{"secondary_y": True}]]
        ).add_trace(
            go.Bar(
                x=df_temp_and_prec["month"],
                y=df_temp_and_prec["total_prec"],
                name="Precipitaciones (mm)",
                marker=dict(color="royalblue")
            ),
            secondary_y=False
        ).add_trace(
            go.Scatter(
                x=df_temp_and_prec["month"],
                y=df_temp_and_prec["avg_tmed"],
                name="Temperatura Media (°C)",
                mode="lines+markers",
                line=dict(color="firebrick", width=2),
                marker=dict(size=6)
            ),
            secondary_y=True
        ).update_layout(
            xaxis=dict(
                tickmode="array",
                tickvals=self.dataframe_temp_and_prec["month"],
                ticktext=list(map(lambda x: x[:3].capitalize(), cts.MONTHS_SP)),
                title="Mes"
            )
        ).update_yaxes(
            title_text="Precipitaciones (mm)",
            range=[
                (self.dataframe_temp_and_prec["avg_tmed"].min() - 5) * factor if self.dataframe_temp_and_prec["avg_tmed"].min() < 0 else 0,
                visual_max
            ],
            dtick=dtick_prec,
            showgrid=True,
            secondary_y=False,
            tickmode="linear"
        ).update_yaxes(
            title_text="Temperatura Media (°C)",
            range=[
                self.dataframe_temp_and_prec["avg_tmed"].min() - 5 if self.dataframe_temp_and_prec["avg_tmed"].min() < 0 else 0,
                visual_max / factor
            ],
            dtick=dtick_temp,
            showgrid=False,
            secondary_y=True,
            tickmode="linear"
        ).update_layout(
            title=f"Climograma 2024 {df_station["indicativo"].iloc[0]} ({self.climate})<br><sup>{df_station["nombre"].iloc[0]} ({df_station["provincia"].iloc[0]})</sup>",
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.15,
                xanchor="center",
                x=0.5
            ),
            margin=dict(t=80, b=80)
        )

    def save_plot(self):
        os.makedirs(self.path_to_save, exist_ok=True)
        self.figure.write_image(self.path_to_save + self.dataframe_station["indicativo"].iloc[0] + ".png")
        self.figure.write_html(self.path_to_save + self.dataframe_station["indicativo"].iloc[0] + ".html")

        return self.path_to_save


# c = ClimographPlotter(
#     "climograph/arid_climates/BWh/canary_islands/temp_and_prec",
#     "climograph/arid_climates/BWh/canary_islands/station",
#     "arid_climates",
#     "BWk",
#     "canary_islands"
# )
#
# c.save_plot()