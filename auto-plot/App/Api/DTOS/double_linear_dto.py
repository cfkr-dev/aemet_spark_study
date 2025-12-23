"""
DTOs for DoubleLinear endpoint.

Defines the request payload model (flask-restx) expected by the double-linear API.

.. module:: App.Api.DTOS.double_linear_dto
"""

from flask_restx import fields, Namespace

from App.Config.constants import FORMATTERS_LIST


class DoubleLinearDTO:
    """Container for the DoubleLinear endpoint request model.

    Exposes `post_input` which is a flask-restx model describing the expected JSON
    payload for POST requests to the double-linear endpoint.

    :param ns: The flask-restx Namespace used to register models and routes.
    :type ns: flask_restx.Namespace
    """
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    """Create and return the flask-restx model describing the POST input payload.

    :param ns: The flask-restx Namespace used to register the model.
    :type ns: flask_restx.Namespace
    :returns: A flask-restx model describing the double-linear POST payload.
    """
    return ns.model('DoubleLinearInput', {
        'src': fields.Nested(ns.model('DoubleLinearSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            'axis': fields.Nested(ns.model('DoubleLinearSrcAxis', {
                'x': fields.Nested(ns.model('DoubleLinearSrcAxisX', {
                    'name': fields.String(reqired=True, description="X Column name"),
                    'format': fields.String(reqired=False, description="Column format", enum=FORMATTERS_LIST),
                }), required=True),
                'y_1': fields.Nested(ns.model('DoubleLinearSrcAxisY1', {
                    'name': fields.String(reqired=True, description="Y 1 Column name"),
                }), required=True),
                'y_2': fields.Nested(ns.model('DoubleLinearSrcAxisY2', {
                    'name': fields.String(reqired=True, description="Y 2 Column name"),
                }), required=True)
            }), required=True)
        })),
        'dest': fields.Nested(ns.model('DoubleLinearDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('DoubleLinearStyle', {
            'lettering': fields.Nested(ns.model('DoubleLinearStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart"),
                'x_label': fields.String(required=True, description="X axis label"),
                'y_1_label': fields.String(required=True, description="Y 1 axis label"),
                'y_2_label': fields.String(required=True, description="Y 2 axis label"),
            }), required=True),
            'figure_1': fields.Nested(ns.model('DoubleLinearStyleFigure1', {
                'name': fields.String(required=True, description="Figure 1 name"),
                'color': fields.String(required=True, description="Figure 1 color"),
            }), required=True),
            'figure_2': fields.Nested(ns.model('DoubleLinearStyleFigure2', {
                'name': fields.String(required=True, description="Figure 2 name"),
                'color': fields.String(required=True, description="Figure 2 color"),
            }), required=True),
            'margin': fields.Nested(ns.model('DoubleLinearStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False),
            'legend': fields.Nested(ns.model('DoubleLinearStyleLegend', {
                'y_offset': fields.Float(required=True, description="Legend Y axis offset"),
            }), required=False),
        }), required=True)
    })
