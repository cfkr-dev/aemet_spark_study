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
    @ns.expect(table_dto.post_input, validate=True)
    def post(self):
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
