"""
Double linear chart controller.

Handles POST requests that create a double stacked linear chart.

.. module:: App.Api.Controllers.double_linear_controller
"""

from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.double_linear_dto import DoubleLinearDTO
from App.Api.Models.double_linear_model import DoubleLinearModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.double_linear_plotter import DoubleLinearPlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('double-linear', description='Create a double linear chart')
double_linear_dto = DoubleLinearDTO(ns)

@ns.route('')
class DoubleLinearController(Resource):
    """Controller to generate a double linear plot from input payload.

    :cvar double_linear_dto: DTO describing expected POST payload.
    """
    @ns.expect(double_linear_dto.post_input, validate=True)
    def post(self):
        """Process request, validate data and create the double linear plot.

        :returns: JSON object with `dest_path` of saved resource.
        :rtype: dict
        :raises werkzeug.exceptions.BadRequest: If model validation fails.
        """
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = DoubleLinearModel(storage)
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        double_linear_plotter = DoubleLinearPlotter(model)

        figure = double_linear_plotter.create_plot()
        dest_path = double_linear_plotter.save_plot(figure)

        return {'dest_path': dest_path}
