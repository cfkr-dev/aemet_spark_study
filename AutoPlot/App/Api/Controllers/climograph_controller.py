from flask import current_app
from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.climograph_dto import ClimographDTO
from App.Api.Models.climograph_model import ClimographModel
from App.Config.constants import AWS_S3_ENDPOINT, STORAGE_PREFIX
from App.Plotters.climograph_plotter import ClimographPlotter
from App.Utils.Storage.Core.storage import Storage

ns = Namespace('climograph', description='Create a climograph chart')
climograph_dto = ClimographDTO(ns)

@ns.route('')
class ClimographController(Resource):
    @ns.expect(climograph_dto.post_input, validate=True)
    def post(self):
        storage = Storage(STORAGE_PREFIX, AWS_S3_ENDPOINT)
        model = ClimographModel(storage)
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        climograph_plotter = ClimographPlotter(model)

        figure = climograph_plotter.create_plot()
        dest_path = climograph_plotter.save_plot(figure)

        return {'dest_path': dest_path}
