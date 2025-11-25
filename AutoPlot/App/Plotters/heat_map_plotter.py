import base64
from pathlib import Path

import cv2
import geopandas as gpd
import numpy as np
import plotly.graph_objects as go
import rasterio
from plotly.graph_objs import Figure
from pykrige.ok import OrdinaryKriging
from rasterio.features import geometry_mask
from scipy.spatial import cKDTree

from App.Api.Models.heat_map_model import HeatMapModel
from App.Config.constants import (
    STORAGE_RESOURCES_COUNTRIES_SHAPEFILE,
    LONG_LOWER_BOUND_SPAIN_CONTINENTAL,
    LONG_UPPER_BOUND_SPAIN_CONTINENTAL,
    LAT_LOWER_BOUND_SPAIN_CONTINENTAL,
    LAT_UPPER_BOUND_SPAIN_CONTINENTAL,
    LONG_LOWER_BOUND_SPAIN_CANARY_ISLAND,
    LONG_UPPER_BOUND_SPAIN_CANARY_ISLAND,
    LAT_LOWER_BOUND_SPAIN_CANARY_ISLAND,
    LAT_UPPER_BOUND_SPAIN_CANARY_ISLAND,
)
from App.Config.enumerations import SpainGeographicLocations
from App.Plotters.abstract_plotter import Plotter
from App.Utils.Storage.Core.storage import Storage
from App.Utils.Storage.PlotExport.plot_export_storage_backend import PlotExportStorageBackend
from App.Utils.file_utils import get_response_dest_path


class HeatMapPlotter(Plotter):
    def __init__(self, heat_map_model: HeatMapModel):
        self.storage = heat_map_model.storage
        self.model = heat_map_model
        self.dataframe = self.load_dataframe(heat_map_model.src.path, heat_map_model.storage)

    def _generate_point_info(self):
        long_col = self.model.src.names.longitude
        lat_col = self.model.src.names.latitude
        style = self.model.style

        point_infos = {}

        for _, row in self.dataframe.iterrows():
            text = ""
            for info_item in style.lettering.point_info:
                text += info_item.label + ": "
                for build_item in info_item.build:
                    text += build_item.text_before + str(row[build_item.name]) + build_item.text_after

                text += "<br>"

            point_infos[(row[long_col], row[lat_col])] = text

        return point_infos


    def create_plot(self):
        long_col = self.model.src.names.longitude
        lat_col = self.model.src.names.latitude
        value_col = self.model.src.names.value
        style = self.model.style

        # Constants
        knn_max_dist = 0.05
        interpolation_grid_size = 1000 if self.model.src.location == SpainGeographicLocations.CONTINENTAL else 2000

        if self.model.src.location == SpainGeographicLocations.CONTINENTAL:
            long_lb, long_ub = LONG_LOWER_BOUND_SPAIN_CONTINENTAL, LONG_UPPER_BOUND_SPAIN_CONTINENTAL
            lat_lb, lat_ub = LAT_LOWER_BOUND_SPAIN_CONTINENTAL, LAT_UPPER_BOUND_SPAIN_CONTINENTAL
        else:
            long_lb, long_ub = LONG_LOWER_BOUND_SPAIN_CANARY_ISLAND, LONG_UPPER_BOUND_SPAIN_CANARY_ISLAND
            lat_lb, lat_ub = LAT_LOWER_BOUND_SPAIN_CANARY_ISLAND, LAT_UPPER_BOUND_SPAIN_CANARY_ISLAND

        # Load Spain shapefile
        world = gpd.read_file(str(STORAGE_RESOURCES_COUNTRIES_SHAPEFILE))
        spain_shape = world[world["SOVEREIGNT"] == "Spain"].geometry.union_all()

        # Load dataframe data
        long_coords_list = []
        lat_coords_list = []
        values_list = []
        for _, row in self.dataframe.iterrows():
            long_coords_list.append(float(row[long_col]))
            lat_coords_list.append(float(row[lat_col]))
            values_list.append(float(row[value_col]))

        long_coords = np.array(long_coords_list)
        lat_coords = np.array(lat_coords_list)
        values = np.array(values_list)

        long_lat_to_point_infos = self._generate_point_info()

        # Filter near points using KNN algorithm
        tree = cKDTree(np.column_stack((long_coords, lat_coords)))
        filtered_idx = tree.query_ball_tree(tree, knn_max_dist)
        unique_idx = np.unique([min(i) for i in filtered_idx])
        filtered_long_coords, filtered_lat_coords, filtered_values = long_coords[unique_idx], lat_coords[unique_idx], \
            values[unique_idx]

        filtered_point_infos = [long_lat_to_point_infos.get((long, lat)) for long, lat in zip(filtered_long_coords, filtered_lat_coords)]

        # Create interpolation grid
        grid_x = np.linspace(long_lb, long_ub, interpolation_grid_size)
        grid_y = np.linspace(lat_lb, lat_ub, interpolation_grid_size)

        # Kriging interpolation
        kriging = OrdinaryKriging(
            filtered_long_coords,
            filtered_lat_coords,
            filtered_values,
            variogram_model='spherical',
            variogram_parameters={
                'sill': 10,
                'range': 2,
                'nugget': 0.1
            }
        )

        interpolated_grid = np.zeros((grid_y.size, grid_x.size), dtype=np.float32)
        block_size = 200
        for i in range(0, grid_y.size, block_size):
            y_block = grid_y[i:i + block_size]
            grid_block = kriging.execute('grid', grid_x, y_block)[0]
            interpolated_grid[i:i + block_size, :] = grid_block

        # Apply frontier mask
        transform = rasterio.transform.from_origin(grid_x.min(), grid_y.max(), (grid_x[1] - grid_x[0]),
                                                   (grid_y[1] - grid_y[0]))
        mask = geometry_mask([spain_shape], transform=transform, invert=True, out_shape=(len(grid_y), len(grid_x)))[
               ::-1]
        interpolated_grid[~mask] = np.nan

        # Generate heat map image in base64
        nan_mask = np.isnan(interpolated_grid)
        min_val = np.nanmin(interpolated_grid)
        max_val = np.nanmax(interpolated_grid)

        norm_grid = (interpolated_grid - min_val) / (max_val - min_val)
        norm_grid[nan_mask] = 0
        img_gray = (norm_grid * 255).astype(np.uint8)

        img_color = cv2.applyColorMap(img_gray, cv2.COLORMAP_JET)
        img_color[nan_mask] = [255, 255, 255]

        img_bgra = cv2.cvtColor(img_color, cv2.COLOR_BGR2BGRA)
        img_bgra[:, :, 3] = 255

        nan_mask_uint8 = np.where(nan_mask, 0, 255).astype(np.uint8)
        contours, _ = cv2.findContours(nan_mask_uint8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cv2.drawContours(img_bgra, contours, -1, (0, 0, 0, 255), thickness=1)

        img_bgra = cv2.flip(img_bgra, 0)

        _, buffer = cv2.imencode('.png', img_bgra)
        img_bytes = buffer.tobytes()
        img_base64 = base64.b64encode(img_bytes).decode()

        # Create Plotly figure
        return go.Figure().add_trace(
            go.Scatter(
                x=filtered_long_coords,
                y=filtered_lat_coords,
                mode='markers',
                marker=dict(
                    size=style.figure.point_size,
                    color=filtered_values,
                    colorscale='jet',
                    colorbar=dict(title=style.lettering.legend_label),
                    opacity=0
                ),
                name=style.figure.name,
                showlegend=True,
                hoverinfo='skip'
            )
        ).add_trace(
            go.Scatter(
                x=filtered_long_coords,
                y=filtered_lat_coords,
                mode='markers',
                marker=dict(
                    size=style.figure.point_size,
                    color=style.figure.color,
                    opacity=style.figure.color_opacity
                ),
                hovertext=filtered_point_infos,
                name=style.figure.name,
                showlegend=False,
                hoverlabel=dict(namelength=0)
            )
        ).add_layout_image(
            dict(
                source="data:image/png;base64," + img_base64,
                xref="x",
                yref="y",
                x=long_lb,
                y=lat_ub,
                sizex=abs(long_lb) + abs(long_ub) if self.model.src.location == SpainGeographicLocations.CONTINENTAL else abs(long_lb) - abs(long_ub),
                sizey=abs(lat_ub) - abs(lat_lb),
                sizing="stretch",
                opacity=0.5,
                layer="below"
            )
        ).update_xaxes(
            title=style.lettering.long_label,
            autorange=False,
            range=[long_lb, long_ub],
            dtick=1,
            showline=True,
            linecolor='black',
            showgrid=True,
            gridcolor='rgba(0,0,0,0.15)',
            zeroline=False,
            mirror="allticks"
        ).update_yaxes(
            title=style.lettering.lat_label,
            autorange=False,
            range=[lat_lb, lat_ub],
            dtick=1,
            constrain='domain',
            scaleanchor="x",
            scaleratio=1,
            showline=True,
            linecolor='black',
            showgrid=True,
            gridcolor='rgba(0,0,0,0.15)',
            zeroline=False,
            mirror="allticks"
        ).update_layout(
            title=f"{style.lettering.title}<br><sup>{style.lettering.subtitle}</sup>" if style.lettering.subtitle else f"{style.lettering.title}",
            autosize=True,
            margin=dict(l=style.margin.left, r=style.margin.right, t=style.margin.top, b=style.margin.bottom),
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=False
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
