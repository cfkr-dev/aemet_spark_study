"""
Pie controller module.

Provides endpoint to create and export pie charts.

.. module:: App.Api.Controllers.pie_controller
"""

from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.pie_dto import PieDTO
from App.Api.Models.pie_model import PieModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.pie_plotter import PiePlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('pie', description='Create a simple pie chart')
pie_dto = PieDTO(ns)

@ns.route('')
class PieController(Resource):
    """Controller exposing POST endpoint to create pie charts."""
    @ns.expect(pie_dto.post_input, validate=True)
    def post(self):
        """Validate request, render pie chart and persist output.

        :returns: JSON object with `dest_path` of exported file.
        :rtype: dict
        :raises werkzeug.exceptions.BadRequest: If validation fails.
        """
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = PieModel(storage)
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        pie_plotter = PiePlotter(model)

        figure = pie_plotter.create_plot()
        dest_path = pie_plotter.save_plot(figure)

        return {'dest_path': dest_path}
