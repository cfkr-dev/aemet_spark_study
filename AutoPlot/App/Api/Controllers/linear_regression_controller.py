from flask_restx import Resource, abort, Namespace
from flask import current_app

from App.Api.DTOS.linear_regression_dto import LinearRegressionDTO
from App.Api.Models.linear_regression_model import LinearRegressionModel
from App.Plotters.linear_regression_plotter import LinearRegressionPlotter

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
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        linear_regression_plotter = LinearRegressionPlotter(model)

        figure = linear_regression_plotter.create_plot()
        dest_path = linear_regression_plotter.save_plot(figure)

        return {'dest_path': dest_path}
