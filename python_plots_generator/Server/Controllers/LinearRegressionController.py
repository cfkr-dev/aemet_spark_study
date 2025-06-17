from flask_restx import Resource, abort, Namespace

from Server.DTOS.LinearRegressionDTO import LinearRegressionDTO

from Server.Models.LinearRegressionModel import LinearRegressionModel
from Plotters.LinearRegressionPlotter import LinearRegressionPlotter

ns = Namespace('linear-regression', description='Create a linear chart with a regression line')
linear_regression_dto = LinearRegressionDTO(ns)

@ns.route('')
class LinearController(Resource):
    @ns.expect(linear_regression_dto.post_input, validate=True)
    def post(self):
        model = LinearRegressionModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        linear_regression_plotter = LinearRegressionPlotter(model)

        figure = linear_regression_plotter.create_plot()
        dest_path = linear_regression_plotter.save_plot(figure)

        return {'dest_path': dest_path}
