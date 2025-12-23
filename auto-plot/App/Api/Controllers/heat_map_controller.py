"""
Heat map controller module.

Provides endpoint to create and export spatial heat maps.

.. module:: App.Api.Controllers.heat_map_controller
"""

from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.heat_map_dto import HeatMapDTO
from App.Api.Models.heat_map_model import HeatMapModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.heat_map_plotter import HeatMapPlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('heat-map', description='Create a heat map chart of spanish continental or canary island territory')
heat_map_dto = HeatMapDTO(ns)

@ns.route('')
class HeatMapController(Resource):
    """Controller responsible for heat map generation requests."""
    @ns.expect(heat_map_dto.post_input, validate=True)
    def post(self):
        """Validate request payload, render a heat map and persist the exported files.

        :returns: JSON object with `dest_path` pointing to exported resource.
        :rtype: dict
        :raises werkzeug.exceptions.BadRequest: If payload validation fails.
        """
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = HeatMapModel(storage)
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        heat_map_plotter = HeatMapPlotter(model)

        figure = heat_map_plotter.create_plot()
        dest_path = heat_map_plotter.save_plot(figure)

        return {'dest_path': dest_path}
