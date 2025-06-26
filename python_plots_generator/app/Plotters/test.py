import base64
import cv2
import geopandas as gpd
import numpy as np
import plotly.graph_objects as go
import rasterio
from pykrige.ok import OrdinaryKriging
from rasterio.features import geometry_mask
from scipy.spatial import cKDTree






import base64
import random
import string
import time

import cv2
import geopandas as gpd
import numpy as np
import plotly.graph_objects as go
import rasterio
from pykrige.ok import OrdinaryKriging
from rasterio.features import geometry_mask
from scipy.spatial import cKDTree
from shapely.geometry import Point

# --- Configuración ---
FILTRAR_PUNTOS = True

# --- Medición de tiempo ---
def medir_tiempo(etiqueta, funcion, *args, **kwargs):
    inicio = time.time()
    resultado = funcion(*args, **kwargs)
    fin = time.time()
    print(f"{etiqueta}: {fin - inicio:.2f} segundos", flush=True)
    return resultado

# --- 1. Cargar el contorno de España ---
def cargar_contorno():
    world = gpd.read_file(r"C:\Users\alber\PycharmProjects\PythonProject\countries")
    espana = world[world["SOVEREIGNT"] == "Spain"]
    return espana.geometry.unary_union

espana_shape = medir_tiempo("Cargar contorno", cargar_contorno)

# --- 2. Generar puntos con valores adecuados y ID aleatorio ---
def generar_id_aleatorio(longitud=4):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=longitud))

np.random.seed(42)
random.seed(42)
num_puntos = 890
puntos = []
valores = []
ids = []

inicio = time.time()
for _ in range(num_puntos):
    while True:
        lat = np.random.uniform(35.0, 44.0)
        lon = np.random.uniform(-9.5, 4.5)
        punto = Point(lon, lat)

        if espana_shape.contains(punto):
            if -9.0 < lon < -6.0 and 41.0 < lat < 44.0:
                temperatura = np.random.uniform(5, 10)
            elif -7.0 < lon < -4.0 and 42.0 < lat < 44.0:
                temperatura = np.random.uniform(15, 20)
            elif -2.0 < lon < 0.5 and 42.0 < lat < 44.0:
                temperatura = np.random.uniform(5, 10)
            elif 0.5 < lon < 3.0 and 41.0 < lat < 43.0:
                temperatura = np.random.uniform(25, 30)
            elif -6.5 < lon < -2.5 and 40.0 < lat < 42.0:
                temperatura = np.random.uniform(15, 20)
            elif -4.0 < lon < -3.0 and 39.5 < lat < 41.0:
                temperatura = np.random.uniform(25, 30)
            elif -1.0 < lon < 1.5 and 38.0 < lat < 40.0:
                temperatura = np.random.uniform(30, 35)
            elif 1.0 < lon < 4.0 and 38.0 < lat < 40.0:
                temperatura = np.random.uniform(15, 20)
            elif -7.5 < lon < -5.5 and 38.5 < lat < 40.0:
                temperatura = np.random.uniform(30, 35)
            elif -7.5 < lon < -1.5 and 36.0 < lat < 38.5:
                temperatura = np.random.uniform(15, 20)
            else:
                temperatura = np.random.uniform(15, 25)

            puntos.append((lat, lon))
            valores.append(temperatura)
            ids.append(generar_id_aleatorio())
            break
fin = time.time()
print(f"Generar puntos: {fin - inicio:.2f} segundos")

latitudes_ori = np.array([p[0] for p in puntos])
longitudes_ori = np.array([p[1] for p in puntos])
valores_ori = np.array(valores)

# --- Filtrar puntos cercanos ---
def filtrar_puntos(longitudes, latitudes, valores, threshold_distance=0.05):
    tree = cKDTree(np.column_stack((longitudes, latitudes)))
    filtered_idx = tree.query_ball_tree(tree, threshold_distance)
    unique_idx = np.unique([min(i) for i in filtered_idx])
    return longitudes[unique_idx], latitudes[unique_idx], valores[unique_idx]

if FILTRAR_PUNTOS:
    longitudes, latitudes, valores = medir_tiempo("Filtrar puntos cercanos", filtrar_puntos, longitudes_ori, latitudes_ori, valores_ori)

# --- 3. Crear cuadrícula ---
grid_size = 300
grid_x = medir_tiempo("Crear cuadrícula X", np.linspace, -9.5, 4.5, grid_size)
grid_y = medir_tiempo("Crear cuadrícula Y", np.linspace, 35.0, 44.0, grid_size)

# --- 4. Interpolación Kriging ---
def interpolacion_kriging(kriging_class, longitudes, latitudes, valores, grid_x, grid_y):
    variogram_model = 'spherical'
    K = kriging_class(longitudes, latitudes, valores, variogram_model=variogram_model,
                      variogram_parameters={'sill': 10, 'range': 2, 'nugget': 0.1})
    return K.execute('grid', grid_x, grid_y)[0]

interpolated_grid_ok = medir_tiempo("Ordinary Kriging", interpolacion_kriging,
                                    OrdinaryKriging, longitudes, latitudes, valores, grid_x, grid_y)

# --- 5. Aplicar máscara con valor NaN en el mar ---
def aplicar_mascara_nan(grid_x, grid_y, espana_shape, interpolated_grid):
    transform = rasterio.transform.from_origin(grid_x.min(), grid_y.max(),
                                               (grid_x[1] - grid_x[0]), (grid_y[1] - grid_y[0]))
    mask = geometry_mask([espana_shape], transform=transform, invert=True,
                         out_shape=(len(grid_y), len(grid_x)))
    mask = mask[::-1]
    interpolated_grid[~mask] = np.nan
    return interpolated_grid

interpolated_grid_ok = medir_tiempo("Aplicar máscara NaN", aplicar_mascara_nan, grid_x, grid_y, espana_shape, interpolated_grid_ok)

# --- 6. Generar imagen con mar blanco y contorno negro ---
def generar_imagen_cv_mar_blanco(grid):
    nan_mask = np.isnan(grid)
    min_val = np.nanmin(grid)
    max_val = np.nanmax(grid)

    norm_grid = (grid - min_val) / (max_val - min_val)
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
    return img_bgra


def cv2_to_base64(img_bgra):
    _, buffer = cv2.imencode('.png', img_bgra)
    img_bytes = buffer.tobytes()
    return base64.b64encode(img_bytes).decode()

imagen_cv = generar_imagen_cv_mar_blanco(interpolated_grid_ok)
img_base64 = cv2_to_base64(imagen_cv)

fig = go.Figure()

# Trace invisible para la leyenda y colorbar (primero)
fig.add_trace(go.Scatter(
    x=longitudes_ori,
    y=latitudes_ori,
    mode='markers',
    marker=dict(
        size=6,
        color=valores_ori,
        colorscale='jet',
        colorbar=dict(title="Temperatura"),
        opacity=0
    ),
    name='Temperatura',
    showlegend=True,
    hoverinfo='skip'  # No hover en este trace
))

# Puntos negros con hover mostrando ID y coordenadas (después)
fig.add_trace(go.Scatter(
    x=longitudes,
    y=latitudes,
    mode='markers',
    marker=dict(
        size=3,
        color='black',
        opacity=0.5
    ),
    hovertemplate=[
        f"ID: {id_}<br>Lat: {lat:.4f}<br>Lon: {lon:.4f}<extra></extra>"
        for id_, lat, lon in zip(ids, latitudes, longitudes)
    ],
    name='Puntos temperatura',
    showlegend=False,
    hoverlabel=dict(namelength=0)
))

fig.add_layout_image(
    dict(
        source="data:image/png;base64," + img_base64,
        xref="x",
        yref="y",
        x=-9.5,
        y=44.0,
        sizex=9.5 + 4.5,
        sizey=44.0 - 35.0,
        sizing="stretch",
        opacity=0.5,
        layer="below"
    )
)

fig.update_xaxes(
    title="Longitud",
    autorange=False,
    range=[-9.5, 4.5],
    dtick=1,
    showline=True,
    linecolor='black',
    showgrid=True,
    gridcolor='rgba(0,0,0,0.15)',
    zeroline=False,
    mirror="allticks"
)

fig.update_yaxes(
    autorange=False,
    range=[35,44],
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
)

fig.update_layout(
    title="Interpolación de temperatura con mar blanco y borde costero negro",
    autosize=True,
    margin=dict(l=10, r=10, t=40, b=40),
    plot_bgcolor='white',
    paper_bgcolor='white',
    showlegend=False
)

fig.show()










# --- SCRIPT ---

# Constants
KNN_THRESHOLD = 0.05

INTERPOLATION_GRID_SIZE = 1000

LONG_LOWER_BOUND, LONG_UPPER_BOUND = (35.0, 44.0)
LAT_LOWER_BOUND, LAT_UPPER_BOUND = (-9.5, 4.5)

ABS_LONG_LOWER_BOUND, ABS_LONG_UPPER_BOUND = (abs(LONG_LOWER_BOUND), abs(LONG_UPPER_BOUND))
ABS_LAT_LOWER_BOUND, ABS_LAT_UPPER_BOUND = (abs(LAT_LOWER_BOUND), abs(LAT_UPPER_BOUND))

KRIGING_VARIOGRAM_MODEL = 'spherical'
KRIGING_VARIOGRAM_PARAM_SILL, KRIGING_VARIOGRAM_PARAM_RANGE, KRIGING_VARIOGRAM_PARAM_NUGGET = (10, 2, 0.1)
KRIGING_EXECUTION_STYLE = 'grid'

# Load Spain shapefile
world = gpd.read_file(r"C:\Users\alber\PycharmProjects\PythonProject\countries")
spain_shape = world[world["SOVEREIGNT"] == "Spain"].geometry.union_all()

# Load dataframe data
long_coords = np.array()
lat_coords = np.array()
values = np.array()
point_info = []

# Filter near points using KNN algorithm
tree = cKDTree(np.column_stack((long_coords,lat_coords)))
filtered_idx = tree.query_ball_tree(tree, KNN_THRESHOLD)
unique_idx = np.unique([min(i) for i in filtered_idx])
filtered_long_coords, filtered_lat_coords, filtered_values = long_coords[unique_idx], lat_coords[unique_idx], values[unique_idx]

# Create interpolation grid
grid_x = np.linspace(LONG_LOWER_BOUND, LONG_UPPER_BOUND, INTERPOLATION_GRID_SIZE)
grid_y = np.linspace(LAT_LOWER_BOUND, LAT_UPPER_BOUND, INTERPOLATION_GRID_SIZE)

# Kriging interpolation
kriging = OrdinaryKriging(
    filtered_long_coords,
    filtered_lat_coords,
    filtered_values,
    variogram_model = KRIGING_VARIOGRAM_MODEL,
    variogram_parameters = {
        'sill': KRIGING_VARIOGRAM_PARAM_SILL,
        'range': KRIGING_VARIOGRAM_PARAM_RANGE,
        'nugget': KRIGING_VARIOGRAM_PARAM_NUGGET
    }
)
interpolated_grid = kriging.execute(KRIGING_EXECUTION_STYLE, grid_x, grid_y)[0]

# Apply frontier mask
transform = rasterio.transform.from_origin(grid_x.min(), grid_y.max(), (grid_x[1] - grid_x[0]), (grid_y[1] - grid_y[0]))
mask = geometry_mask([spain_shape], transform=transform, invert=True, out_shape=(len(grid_y), len(grid_x)))[::-1]
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
fig = go.Figure().add_trace(
    go.Scatter(
        x=long_coords,
        y=lat_coords,
        mode='markers',
        marker=dict(
            size=6,
            color=values,
            colorscale='jet',
            colorbar=dict(title="Temperatura"),
            opacity=0
        ),
        name='Temperatura',
        showlegend=True,
        hoverinfo='skip'  # No hover en este trace
    )
).add_trace(
    go.Scatter(
        x=filtered_long_coords,
        y=filtered_lat_coords,
        mode='markers',
        marker=dict(
            size=3,
            color='black',
            opacity=0.5
        ),
        hovertemplate=[
            f"ID: {id_}<br>Lat: {lat:.4f}<br>Lon: {lon:.4f}<extra></extra>"
            for id_, lat, lon in zip(ids, latitudes, longitudes)
        ],
        name='Puntos temperatura',
        showlegend=False,
        hoverlabel=dict(namelength=0)
    )
).add_layout_image(
    dict(
        source="data:image/png;base64," + img_base64,
        xref="x",
        yref="y",
        x=LAT_LOWER_BOUND,
        y=LONG_UPPER_BOUND,
        sizex=ABS_LAT_LOWER_BOUND + ABS_LAT_UPPER_BOUND,
        sizey=ABS_LONG_UPPER_BOUND - ABS_LONG_LOWER_BOUND,
        sizing="stretch",
        opacity=0.5,
        layer="below"
    )
).update_xaxes(
    title="Longitud",
    autorange=False,
    range=[LAT_LOWER_BOUND, LAT_UPPER_BOUND],
    dtick=1,
    showline=True,
    linecolor='black',
    showgrid=True,
    gridcolor='rgba(0,0,0,0.15)',
    zeroline=False,
    mirror="allticks"
).update_yaxes(
    autorange=False,
    range=[LONG_LOWER_BOUND, LONG_UPPER_BOUND],
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
    title="Interpolación de temperatura con mar blanco y borde costero negro",
    autosize=True,
    margin=dict(l=10, r=10, t=40, b=40),
    plot_bgcolor='white',
    paper_bgcolor='white',
    showlegend=False
)
