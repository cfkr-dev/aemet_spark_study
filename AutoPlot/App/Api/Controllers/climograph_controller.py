from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.climograph_dto import ClimographDTO
from App.Api.Models.climograph_model import ClimographModel
from App.Plotters.climograph_plotter import ClimographPlotter

ns = Namespace('climograph', description='Create a climograph chart')
climograph_dto = ClimographDTO(ns)

@ns.route('')
class ClimographController(Resource):
    @ns.expect(climograph_dto.post_input, validate=True)
    def post(self):
        model = ClimographModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        climograph_plotter = ClimographPlotter(model)

        figure = climograph_plotter.create_plot()
        dest_path = climograph_plotter.save_plot(figure)

        return {'dest_path': dest_path}
