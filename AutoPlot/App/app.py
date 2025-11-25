import logging

from flask import Flask
from flask_restx import Api

from App.Api.Controllers.health_controller import ns as health_ns
from App.Api.Controllers.linear_controller import ns as linear_ns
from App.Api.Controllers.double_linear_controller import ns as double_linear_ns
from App.Api.Controllers.linear_regression_controller import ns as linear_regression_ns
from App.Api.Controllers.pie_controller import ns as pie_linear_ns
from App.Api.Controllers.bar_controller import ns as bar_model_bar_ns
from App.Api.Controllers.table_controller import ns as table_linear_ns
from App.Api.Controllers.climograph_controller import ns as climograph_ns
from App.Api.Controllers.heat_map_controller import ns as heat_map_ns

# Init Flask App
app = Flask(__name__)
api =  Api(app, version = '1.0', title = 'Autoplot Flask API', description = 'An API in Flask for creating a charts using meteorological data')

# Registry all namespaces
api.add_namespace(health_ns, path='/health')
api.add_namespace(linear_ns, path='/linear')
api.add_namespace(double_linear_ns, path='/double-linear')
api.add_namespace(linear_regression_ns, path='/linear-regression')
api.add_namespace(pie_linear_ns, path='/pie')
api.add_namespace(bar_model_bar_ns, path='/bar')
api.add_namespace(table_linear_ns, path='/table')
api.add_namespace(climograph_ns, path='/climograph')
api.add_namespace(heat_map_ns, path='/heat-map')

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers = gunicorn_error_logger.handlers
app.logger.setLevel(gunicorn_error_logger.level)

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=True)
