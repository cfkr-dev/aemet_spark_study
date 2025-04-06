# import pandas as pd
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots
# import math
#
# # Leer los datos
# df_temp_and_prec = pd.read_parquet("../../data/results/spark/climograph/arid_climates/BSh/peninsula/temp_and_prec/", engine='pyarrow')
#
# # Crear figura con eje Y secundario
# fig = make_subplots(specs=[[{"secondary_y": True}]])
#
# # Añadir barras para precipitaciones (eje izquierdo)
# fig.add_trace(
#     go.Bar(
#         x=df_temp_and_prec["month"],
#         y=df_temp_and_prec["total_prec"],
#         name="Precipitaciones (mm)",
#         marker_color="royalblue"
#     ),
#     secondary_y=False
# )
#
# # Añadir línea con puntos para temperatura (eje derecho)
# fig.add_trace(
#     go.Scatter(
#         x=df_temp_and_prec["month"],
#         y=df_temp_and_prec["avg_tmed"],
#         name="Temperatura Media (°C)",
#         mode="lines+markers",
#         line=dict(color="firebrick", width=2),
#         marker=dict(size=6)
#     ),
#     secondary_y=True
# )
#
# # Etiquetas de los meses
# meses_completos = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
#                    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
#
# fig.update_layout(
#     xaxis=dict(
#         tickmode="array",
#         tickvals=df_temp_and_prec["month"],
#         ticktext=meses_completos
#     ),
#     xaxis_title="Mes",
# )
#
# # Sincronizar escalas perfectamente:
# max_prec = df_temp_and_prec["total_prec"].max()
# max_temp = df_temp_and_prec["avg_tmed"].max()
#
# # Queremos que 1°C = 2mm de precipitación (como un climograma clásico)
# factor = 2
#
# # Escalamos la temperatura con ese factor
# scaled_temp_max = max_temp * factor
#
# # Usamos el mayor entre max_prec y scaled_temp_max para fijar un alto común
# visual_max = max(scaled_temp_max, max_prec)
#
# # Redondeamos visual_max a múltiplo de 10 para estética
# visual_max = math.ceil(visual_max / 10) * 10
#
# # Establecer dtick común
# tick_count = 6
# dtick_prec = visual_max / tick_count
# dtick_temp = dtick_prec / factor
#
# # Eje izquierdo: precipitaciones
# fig.update_yaxes(
#     title_text="Precipitaciones (mm)",
#     range=[0, visual_max],
#     dtick=dtick_prec,
#     side="left",
#     showgrid=True,
#     secondary_y=False
# )
#
# # Eje derecho: temperaturas
# fig.update_yaxes(
#     title_text="Temperatura Media (°C)",
#     range=[0, visual_max / factor],
#     dtick=dtick_temp,
#     side="right",
#     showgrid=False,
#     secondary_y=True
# )
#
# # Título y leyenda
# fig.update_layout(
#     title="Temperaturas y Precipitaciones Medias Mensuales",
#     legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
#     margin=dict(t=50, b=50),
# )
#
# fig.show()






import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import math

# Leer los datos
df = pd.read_parquet(
    "../../data/results/spark/climograph/arid_climates/BSh/peninsula/temp_and_prec/",
    engine='pyarrow'
)

# Crear figura con eje Y secundario
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Añadir precipitaciones (barra)
fig.add_trace(
    go.Bar(
        x=df["month"],
        y=df["total_prec"],
        name="Precipitaciones (mm)",
        marker_color="royalblue"
    ),
    secondary_y=False
)

# Añadir temperaturas (línea + puntos)
fig.add_trace(
    go.Scatter(
        x=df["month"],
        y=df["avg_tmed"],
        name="Temperatura Media (°C)",
        mode="lines+markers",
        line=dict(color="firebrick", width=2),
        marker=dict(size=6)
    ),
    secondary_y=True
)

# Etiquetas de los meses
meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
         "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

fig.update_layout(
    xaxis=dict(
        tickmode="array",
        tickvals=df["month"],
        ticktext=meses,
        title="Mes"
    )
)

# Sincronización visual entre temperatura y precipitación
factor = 2  # 1°C = 2mm
visual_max = math.ceil(max(
    df["total_prec"].max(),
    df["avg_tmed"].max() * factor
) / 10) * 10

dtick_prec = visual_max / 6
dtick_temp = dtick_prec / factor

# Ejes Y
fig.update_yaxes(
    title_text="Precipitaciones (mm)",
    range=[0, visual_max],
    dtick=dtick_prec,
    showgrid=True,
    secondary_y=False
)

fig.update_yaxes(
    title_text="Temperatura Media (°C)",
    range=[0, visual_max / factor],
    dtick=dtick_temp,
    showgrid=False,
    secondary_y=True
)

# Layout final
fig.update_layout(
    title="Temperaturas y Precipitaciones Medias Mensuales",
    legend=dict(
        orientation="h",
        yanchor="top",
        y=-0.3,  # leyenda en la parte inferior
        xanchor="center",
        x=0.5
    ),
    margin=dict(t=80, b=80)  # margen superior aumentado para el título
)

fig.show()


