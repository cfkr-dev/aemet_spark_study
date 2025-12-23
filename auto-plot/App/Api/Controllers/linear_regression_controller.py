"""
Linear regression chart controller.

Handles requests that render a linear chart with regression overlay.

.. module:: App.Api.Controllers.linear_regression_controller
"""

from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.linear_regression_dto import LinearRegressionDTO
from App.Api.Models.linear_regression_model import LinearRegressionModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.linear_regression_plotter import LinearRegressionPlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('linear-regression', description='Create a linear chart with a regression line')
linear_regression_dto = LinearRegressionDTO(ns)

@ns.route('')
class LinearController(Resource):
    """Controller to produce a linear chart with regression line."""
    @ns.expect(linear_regression_dto.post_input, validate=True)
    def post(self):
        """Validate request, render regression chart and save the output.

        :returns: JSON object with exported `dest_path`.
        :rtype: dict
        :raises werkzeug.exceptions.BadRequest: If model validation fails.
        """
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = LinearRegressionModel(storage)
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
