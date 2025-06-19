from flask_restx import Resource, abort, Namespace

from Server.DTOS.TableDTO import TableDTO

from Server.Models.TableModel import TableModel
from Plotters.TablePlotter import TablePlotter

ns = Namespace('table', description='Create a simple table chart')
table_dto = TableDTO(ns)

@ns.route('')
class TableController(Resource):
    @ns.expect(table_dto.post_input, validate=True)
    def post(self):
        model = TableModel()
        model.setup(ns.payload)
        validation = model.validate()

        if not validation.is_valid():
            abort(400, message=validation.build_error_message())

        table_plotter = TablePlotter(model)

        figure = table_plotter.create_plot()
        dest_path = table_plotter.save_plot(figure)

        return {'dest_path': dest_path}
