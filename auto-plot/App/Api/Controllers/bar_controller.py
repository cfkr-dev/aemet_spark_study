"""
Bar controller module.

Provides endpoint to create and export bar charts.

.. module:: App.Api.Controllers.bar_controller
"""

from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.bar_dto import BarDTO
from App.Api.Models.bar_model import BarModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.bar_plotter import BarPlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('bar', description='Create a simple bar chart')
bar_dto = BarDTO(ns)

@ns.route('')
class BarController(Resource):
    """Resource that exposes a POST endpoint to create bar charts.

    :cvar bar_dto: DTO describing the expected payload for POST requests.
    """
    @ns.expect(bar_dto.post_input, validate=True)
    def post(self):
        """Process a POST request, validate input, render the bar chart and persist it.

        :returns: A JSON object containing the `dest_path` key with the exported file path.
        :rtype: dict
        :raises werkzeug.exceptions.BadRequest: If validation fails the endpoint will abort with 400.
        """
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = BarModel(storage)
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        bar_plotter = BarPlotter(model)

        figure = bar_plotter.create_plot()
        dest_path = bar_plotter.save_plot(figure)

        return {'dest_path': dest_path}
