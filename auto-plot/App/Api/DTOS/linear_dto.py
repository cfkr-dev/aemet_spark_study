"""
DTOs for Linear endpoint.

Defines the request payload model (flask-restx) expected by the linear API.

.. module:: App.Api.DTOS.linear_dto
"""

from flask_restx import fields, Namespace

from App.Config.constants import FORMATTERS_LIST


class LinearDTO:
    """Container for the Linear endpoint request model.

    Exposes `post_input` which is a flask-restx model describing the expected JSON
    payload for POST requests to the linear endpoint.

    :param ns: The flask-restx Namespace used to register models and routes.
    :type ns: flask_restx.Namespace
    """
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    """Create and return the flask-restx model describing the POST input payload.

    :param ns: The flask-restx Namespace used to register the model.
    :type ns: flask_restx.Namespace
    :returns: A flask-restx model describing the linear POST payload.
    """
    return ns.model('LinearInput', {
        'src': fields.Nested(ns.model('LinearSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            'axis': fields.Nested(ns.model('LinearSrcAxis', {
                'x': fields.Nested(ns.model('LinearSrcAxisX', {
                    'name': fields.String(reqired=True, description="X Column name"),
                    'format': fields.String(reqired=False, description="Column format", enum=FORMATTERS_LIST),
                }), required=True),
                'y': fields.Nested(ns.model('LinearSrcAxisY', {
                    'name': fields.String(reqired=True, description="Y Column name")
                }), required=True)
            }), required=True)
        })),
        'dest': fields.Nested(ns.model('LinearDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('LinearStyle', {
            'lettering': fields.Nested(ns.model('LinearStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart"),
                'x_label': fields.String(required=True, description="X axis label"),
                'y_label': fields.String(required=True, description="Y axis label"),
            }), required=True),
            'figure': fields.Nested(ns.model('LinearStyleFigure', {
                'name': fields.String(required=True, description="Figure name"),
                'color': fields.String(required=True, description="Figure color"),
            }), required=True),
            'margin': fields.Nested(ns.model('LinearStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False),
            'legend': fields.Nested(ns.model('LinearStyleLegend', {
                'y_offset': fields.Float(required=True, description="Legend Y axis offset"),
            }), required=False),
        }), required=True)
    })
