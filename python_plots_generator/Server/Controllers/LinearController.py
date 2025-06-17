from flask_restx import Resource, abort, Namespace

from Server.DTOS.LinearDTO import LinearDTO

from Server.Models.LinearModel import LinearModel
from Plotters.LinearPlotter import LinearPlotter

ns = Namespace('linear', description='Create a simple linear chart')
linear_dto = LinearDTO(ns)

@ns.route('')
class LinearController(Resource):
    @ns.expect(linear_dto.post_input, validate=True)
    def post(self):
        model = LinearModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        linear_plotter = LinearPlotter(model)

        figure = linear_plotter.create_plot()
        dest_path = linear_plotter.save_plot(figure)

        return {'dest_path': dest_path}
