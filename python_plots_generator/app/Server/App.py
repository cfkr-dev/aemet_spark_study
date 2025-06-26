from flask import Flask
from flask_restx import Api

from Controllers.LinearController import ns as linear_ns
from Controllers.DoubleLinearController import ns as double_linear_ns
from Controllers.LinearRegressionController import ns as linear_regression_ns
from Controllers.PieController import ns as pie_linear_ns
from Controllers.BarController import ns as bar_model_bar_ns
from Controllers.TableController import ns as table_linear_ns
from Controllers.ClimographController import ns as climograph_ns
from Controllers.HeatMapController import ns as heat_map_ns

# Init Flask App
app = Flask(__name__)
api =  Api(app, version = '1.0', title = 'Autoplot Flask API', description = 'An API in Flask for creating a charts using meteorological data')

# Registry all namespaces
api.add_namespace(linear_ns, path='/linear')
api.add_namespace(double_linear_ns, path='/double-linear')
api.add_namespace(linear_regression_ns, path='/linear-regression')
api.add_namespace(pie_linear_ns, path='/pie')
api.add_namespace(bar_model_bar_ns, path='/bar')
api.add_namespace(table_linear_ns, path='/table')
api.add_namespace(climograph_ns, path='/climograph')
api.add_namespace(heat_map_ns, path='/heat-map')

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=False)
