from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.linear_dto import LinearDTO
from App.Api.Models.linear_model import LinearModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.linear_plotter import LinearPlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('linear', description='Create a simple linear chart')
linear_dto = LinearDTO(ns)

@ns.route('')
class LinearController(Resource):
    @ns.expect(linear_dto.post_input, validate=True)
    def post(self):
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = LinearModel(storage)
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        linear_plotter = LinearPlotter(model)

        figure = linear_plotter.create_plot()
        dest_path = linear_plotter.save_plot(figure)

        return {'dest_path': dest_path}
