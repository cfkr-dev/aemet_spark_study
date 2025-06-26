from flask_restx import Resource, abort, Namespace

from app.Server.DTOS.HeatMapDTO import HeatMapDTO

from app.Server.Models.HeatMapModel import HeatMapModel
from app.Plotters.HeatMapPlotter import HeatMapPlotter

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
