from flask_restx import Resource, abort, Namespace
from flask import current_app

from App.Api.DTOS.double_linear_dto import DoubleLinearDTO
from App.Api.Models.double_linear_model import DoubleLinearModel
from App.Plotters.double_linear_plotter import DoubleLinearPlotter

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
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        double_linear_plotter = DoubleLinearPlotter(model)

        figure = double_linear_plotter.create_plot()
        dest_path = double_linear_plotter.save_plot(figure)

        return {'dest_path': dest_path}
