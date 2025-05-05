from flask import Flask, request, jsonify

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

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=False)
