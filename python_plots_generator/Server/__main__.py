from flask import Flask, request, jsonify

from PlotterStyles import LinearPlotterStyle
from Plotters import LinearPlotter
from Plotters.climograph import ClimographPlotter

# Crear la aplicación Flask
app = Flask(__name__)

# Endpoint POST
@app.route('/plots/generate/climograph', methods=['POST'])
def mi_endpoint():
    data = request.get_json()

    if not data:
        return jsonify({'error': 'No se enviaron datos JSON'}), 400

    temp_and_prec = data.get('source_paths').get('temp_and_prec')
    station = data.get('source_paths').get('station')
    climate_group = data.get('specific_info').get('climate_group')
    climate = data.get('specific_info').get('climate')
    location = data.get('specific_info').get('location')

    plotter = ClimographPlotter(temp_and_prec, station, climate_group, climate, location)
    path = plotter.save_plot()

    return jsonify({
        'operation_status': 'completed',
        'resource_path': path
    }), 200

@app.route('/plots/generate/linear', methods=['POST'])
def linear_endpoint():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No se enviaron datos JSON'}), 400

    """
    {
        "src_path": "/temp/evol/cadiz/evol",
        "src_cols": {
            "x": "date",
            "y": "temp_daily_avg"
        },
        "formats": {
            "date": "timestamp"
        },
        "dest_path": "/test",
        "plot_filename": "plot_test",
        "style": {
            "title": "EVOLUTION TEST",
            "xlabel": "Date",
            "ylabel": "Temperature (ºC)",
            "figure_name": "Temperature",
            "figure_color": "royalblue",
            "show_legend": true
        }
    }
    """

    src_path = data.get('src_path')
    src_col_x = data.get('src_cols').get('x')
    src_col_y = data.get('src_cols').get('y')
    formats = data.get('formats')
    dest_path = data.get('dest_path')
    plot_filename = data.get('plot_filename')
    style = data.get('style')

    plotter = LinearPlotter(
        src_path,
        src_col_x,
        src_col_y,
        formats,
        dest_path,
        plot_filename,
        LinearPlotterStyle()
        .set_title(style.get('title'))
        .set_xaxis_label(style.get('xlabel'))
        .set_yaxis_label(style.get('ylabel'))
        .set_figure_name(style.get('figure_name'))
        .set_figure_color(style.get('figure_color'))
        .set_show_legend(style.get('show_legend'))
    )

    path = plotter.save_plot()

    return jsonify({
        'operation_status': 'completed',
        'resource_path': path
    }), 200

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=False)
