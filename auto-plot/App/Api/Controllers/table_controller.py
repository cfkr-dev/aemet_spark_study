"""
Table controller module.

Provides an API endpoint to create and export a table visualization.

.. module:: App.Api.Controllers.table_controller
"""

from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.table_dto import TableDTO
from App.Api.Models.table_model import TableModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.table_plotter import TablePlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('table', description='Create a simple table chart')
table_dto = TableDTO(ns)

@ns.route('')
class TableController(Resource):
    """Resource for generating table charts.

    Exposes a POST endpoint that accepts a JSON payload describing source, destination
    and styling for a table chart. On success it returns the export destination path.

    """
    @ns.expect(table_dto.post_input, validate=True)
    def post(self):
        """Handle POST requests to generate and save a table chart.

        The endpoint will:
        - validate and parse the incoming payload using `TableDTO`,
        - build a `TableModel` and validate it,
        - create a `TablePlotter` to render the figure,
        - persist the figure and return the destination path.

        :returns: A JSON object containing the `dest_path` key with the exported file path.
        :rtype: dict
        :raises werkzeug.exceptions.BadRequest: If validation fails the endpoint will abort with 400.
        """
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = TableModel(storage)
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        table_plotter = TablePlotter(model)

        figure = table_plotter.create_plot()
        dest_path = table_plotter.save_plot(figure)

        return {'dest_path': dest_path}
