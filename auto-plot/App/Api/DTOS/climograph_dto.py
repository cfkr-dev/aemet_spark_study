"""
DTOs for Climograph endpoint.

Defines the request payload model (flask-restx) expected by the climograph API.

.. module:: App.Api.DTOS.climograph_dto
"""

from flask_restx import fields, Namespace


class ClimographDTO:
    """Container for the Climograph endpoint request model.

    Exposes `post_input` which is a flask-restx model describing the expected JSON
    payload for POST requests to the climograph endpoint.

    :param ns: The flask-restx Namespace used to register models and routes.
    :type ns: flask_restx.Namespace
    """
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    """Create and return the flask-restx model describing the POST input payload.

    :param ns: The flask-restx Namespace used to register the model.
    :type ns: flask_restx.Namespace
    :returns: A flask-restx model describing the climograph POST payload.
    """
    return ns.model('ClimographInput', {
        'src': fields.Nested(ns.model('ClimographSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            'axis': fields.Nested(ns.model('ClimographSrcAxis', {
                'x': fields.Nested(ns.model('ClimographSrcAxisX', {
                    'name': fields.String(reqired=True, description="X Column name"),
                }), required=True),
                'y_temp': fields.Nested(ns.model('ClimographSrcAxisYTemp', {
                    'name': fields.String(reqired=True, description="Y Temperature Column name"),
                }), required=True),
                'y_prec': fields.Nested(ns.model('ClimographSrcAxisYPrecipitation', {
                    'name': fields.String(reqired=True, description="Y Precipitation Column name"),
                }), required=True)
            }), required=True)
        })),
        'dest': fields.Nested(ns.model('ClimographDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('ClimographStyle', {
            'lettering': fields.Nested(ns.model('ClimographStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart"),
                'x_label': fields.String(required=True, description="X axis label"),
                'y_temp_label': fields.String(required=True, description="Y Temperature axis label"),
                'y_prec_label': fields.String(required=True, description="Y Precipitation axis label"),
            }), required=True),
            'figure_temp': fields.Nested(ns.model('ClimographStyleFigureTemp', {
                'name': fields.String(required=True, description="Figure temperature name"),
                'color': fields.String(required=True, description="Figure temperature color"),
            }), required=True),
            'figure_prec': fields.Nested(ns.model('ClimographStyleFigurePrec', {
                'name': fields.String(required=True, description="Figure precipitation name"),
                'color': fields.String(required=True, description="Figure precipitation color"),
            }), required=True),
            'margin': fields.Nested(ns.model('ClimographStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False),
            'legend': fields.Nested(ns.model('ClimographStyleLegend', {
                'y_offset': fields.Float(required=True, description="Legend Y axis offset"),
            }), required=False),
        }), required=True)
    })
