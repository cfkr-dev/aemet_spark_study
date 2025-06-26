from flask_restx import Resource, abort, Namespace

from app.Server.DTOS.DoubleLinearDTO import DoubleLinearDTO

from app.Server.Models.DoubleLinearModel import DoubleLinearModel
from app.Plotters.DoubleLinearPlotter import DoubleLinearPlotter

ns = Namespace('double-linear', description='Create a double linear chart')
double_linear_dto = DoubleLinearDTO(ns)

@ns.route('')
class DoubleLinearController(Resource):
    @ns.expect(double_linear_dto.post_input, validate=True)
    def post(self):
        model = DoubleLinearModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        double_linear_plotter = DoubleLinearPlotter(model)

        figure = double_linear_plotter.create_plot()
        dest_path = double_linear_plotter.save_plot(figure)

        return {'dest_path': dest_path}
