import pandas as pd
import plotly.graph_objects as go

# Cargar archivo parquet
df = pd.read_parquet("E:/Escritorio/UNIVERSIDAD/TFG/TFG GII - Spark Big Data Study/data/spark/interesting_studies/prec_and_pression_evol/a_coruna/evol")

# Convertir columna date a datetime
df['date'] = pd.to_datetime(df['date'])

# Ordenar por fecha
df = df.sort_values(by='date')

# Crear figura
fig = go.Figure()

# Traza para press_max_daily_avg (eje izquierdo)
fig.add_trace(go.Scatter(
    x=df['date'],
    y=df['press_max_daily_avg'],
    name='Presión Máxima (Diaria)',
    mode='lines',
    line=dict(color='red'),
    yaxis='y1'
))

# Traza para prec_daily_avg (eje derecho)
fig.add_trace(go.Scatter(
    x=df['date'],
    y=df['prec_daily_avg'],
    name='Precipitación Promedio (Diaria)',
    mode='lines',
    line=dict(color='blue'),
    yaxis='y2'
))

# Actualizar diseño
fig.update_layout(
    title='Presión Máxima y Precipitación Promedio Diarias',
    xaxis=dict(title='Fecha'),
    yaxis=dict(
        title=dict(text='Presión Máxima (hPa)', font=dict(color='red')),
        tickfont=dict(color='red'),
        side='left'
    ),
    yaxis2=dict(
        title=dict(text='Precipitación (mm)', font=dict(color='blue')),
        tickfont=dict(color='blue'),
        overlaying='y',
        side='right'
    ),
    template='plotly_white',
    hovermode='x unified',
    legend=dict(x=0.01, y=0.99)
)

fig.show()