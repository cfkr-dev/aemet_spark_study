from flask_restx import Resource, abort, Namespace
from flask import current_app

from App.Api.DTOS.bar_dto import BarDTO
from App.Api.Models.bar_model import BarModel
from App.Plotters.bar_plotter import BarPlotter

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
            error_message = validation.build_error_message()
            current_app.logger.warning(error_message)
            abort(400, message=error_message)

        bar_plotter = BarPlotter(model)

        figure = bar_plotter.create_plot()
        dest_path = bar_plotter.save_plot(figure)

        return {'dest_path': dest_path}
