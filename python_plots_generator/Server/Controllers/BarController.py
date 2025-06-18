from flask_restx import Resource, abort, Namespace

from Server.DTOS.BarDTO import BarDTO

from Server.Models.BarModel import BarModel
from Plotters.BarPlotter import BarPlotter

ns = Namespace('bar', description='Create a simple bar chart')
bar_dto = BarDTO(ns)

@ns.route('')
class BarController(Resource):
    @ns.expect(bar_dto.post_input, validate=True)
    def post(self):
        model = BarModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        bar_plotter = BarPlotter(model)

        figure = bar_plotter.create_plot()
        dest_path = bar_plotter.save_plot(figure)

        return {'dest_path': dest_path}
