"""import plotly.graph_objects as go

# Datos
productos = ['Producto A', 'Producto B', 'Producto C']
ventas = [1200, 900, 1600]
responsables = ['Ana', 'Luis', 'Carlos']

# Creamos el texto adicional que aparecerá sobre cada barra
info_texto = [f'Ventas: {v}<br>Responsable: {r}' for v, r in zip(ventas, responsables)]

# Gráfico de barras
fig = go.Figure(data=[go.Bar(
    x=productos,
    y=ventas,
    text=info_texto,             # Información extra
    textposition='inside',         # Posición automática (dentro o fuera de la barra según espacio)
    marker_color='indianred'     # Color de las barras
)])

# Opcional: título y etiquetas
fig.update_layout(
    title='Ventas por Producto con Información Adicional',
    xaxis_title='Producto',
    yaxis_title='Ventas',
    uniformtext_minsize=8,
    uniformtext_mode='hide'
)

fig.show()"""

import plotly.graph_objects as go

# Datos para la tabla
header = ["País", "Capital", "Población (millones)", "Área (km²)"]
data = [
    ["España", "Madrid", 47.4, 505990],
    ["Francia", "París", 67.8, 551695],
    ["Alemania", "Berlín", 83.2, 357386],
    ["Italia", "Roma", 59.3, 301340],
]

# Transponer datos para columnas
columns = list(zip(*data))

# Crear gráfico tipo tabla
fig = go.Figure(data=[go.Table(
    header=dict(
        values=header,
        align='left',
        font=dict(color='black', size=14)
    ),
    cells=dict(
        values=columns,
        align='left',
        font=dict(color='black', size=12)
    )
)])

fig.update_layout(
    title="Datos de países europeos",
    margin=dict(l=10, r=10, t=50, b=10)
)

fig.show()
