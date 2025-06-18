from flask_restx import Resource, abort, Namespace

from Server.DTOS.PieDTO import PieDTO

from Server.Models.PieModel import PieModel
from Plotters.PiePlotter import PiePlotter

ns = Namespace('pie', description='Create a simple pie chart')
pie_dto = PieDTO(ns)

@ns.route('')
class PieController(Resource):
    @ns.expect(pie_dto.post_input, validate=True)
    def post(self):
        model = PieModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        pie_plotter = PiePlotter(model)

        figure = pie_plotter.create_plot()
        dest_path = pie_plotter.save_plot(figure)

        return {'dest_path': dest_path}
