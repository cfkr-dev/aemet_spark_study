from flask import Flask
from flask_restx import Api

from Controllers.LinearController import ns as linear_ns
from Controllers.DoubleLinearController import ns as double_linear_ns
from Controllers.LinearRegressionController import ns as linear_regression_ns

# Init Flask App
app = Flask(__name__)
api =  Api(app, version = '1.0', title = 'Autoplot Flask API', description = 'An API in Flask for creating a charts using meteorological data')

# Registry all namespaces
api.add_namespace(linear_ns, path='/linear')
api.add_namespace(double_linear_ns, path='/double-linear')
api.add_namespace(linear_regression_ns, path='/linear-regression')

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=False)
