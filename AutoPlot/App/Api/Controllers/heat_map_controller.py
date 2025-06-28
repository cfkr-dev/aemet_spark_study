from flask_restx import Resource, abort, Namespace

from App.Api.DTOS.heat_map_dto import HeatMapDTO
from App.Api.Models.heat_map_model import HeatMapModel
from App.Plotters.heat_map_plotter import HeatMapPlotter

ns = Namespace('heat-map', description='Create a heat map chart of spanish continental or canary island territory')
heat_map_dto = HeatMapDTO(ns)

@ns.route('')
class HeatMapController(Resource):
    @ns.expect(heat_map_dto.post_input, validate=True)
    def post(self):
        model = HeatMapModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        heat_map_plotter = HeatMapPlotter(model)

        figure = heat_map_plotter.create_plot()
        dest_path = heat_map_plotter.save_plot(figure)

        return {'dest_path': dest_path}
