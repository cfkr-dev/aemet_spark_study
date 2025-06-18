import plotly.graph_objects as go

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

fig.show()
