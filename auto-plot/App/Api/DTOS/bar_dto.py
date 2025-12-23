"""
DTOs for Bar endpoint.

Defines the request payload model (flask-restx) expected by the bar API.

.. module:: App.Api.DTOS.bar_dto
"""

from flask_restx import fields, Namespace


class BarDTO:
    """Container for the Bar endpoint request model.

    Exposes `post_input` which is a flask-restx model describing the expected JSON
    payload for POST requests to the bar endpoint.

    :param ns: The flask-restx Namespace used to register models and routes.
    :type ns: flask_restx.Namespace
    """
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    """Create and return the flask-restx model describing the POST input payload.

    :param ns: The flask-restx Namespace used to register the model.
    :type ns: flask_restx.Namespace
    :returns: A flask-restx model describing the bar POST payload.
    """
    return ns.model('BarInput', {
        'src': fields.Nested(ns.model('BarSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            'axis': fields.Nested(ns.model('BarSrcAxis', {
                'x': fields.Nested(ns.model('BarSrcAxisX', {
                    'name': fields.String(reqired=True, description="X Column name")
                }), required=True),
                'y': fields.Nested(ns.model('BarSrcAxisY', {
                    'name': fields.String(reqired=True, description="Y Column name")
                }), required=True)
            }), required=True)
        })),
        'dest': fields.Nested(ns.model('BarDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('BarStyle', {
            'lettering': fields.Nested(ns.model('BarStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart"),
                'x_label': fields.String(required=True, description="X axis label"),
                'y_label': fields.String(required=True, description="Y axis label"),
                "inside_info": fields.List(fields.Nested(ns.model('BarStyleLetteringInsideInfoItem', {
                    'label': fields.String(required=True, description="Label for information"),
                    'build': fields.List(fields.Nested(ns.model('BarStyleLetteringInsideInfoItemBuildItem', {
                        'text_before': fields.String(required=True, description="Text before"),
                        'name': fields.String(required=True, description="Name of the column"),
                        'text_after': fields.String(required=True, description="Text after"),
                    })), required=True)
                })), required=False),
            }), required=True),
            'figure': fields.Nested(ns.model('BarStyleFigure', {
                'inverted_horizontal_axis': fields.Boolean(required=True, description="Inverted horizontal axis"),
                'threshold_limit_max_min': fields.Float(required=True,
                                                        description="Threshold limit of extension between the maximum and minimum value to zoom the chart"),
                'threshold_perc_limit_outside_text': fields.Float(required=True,
                                                                  description="Threshold percentage limit for putting text outside the bars"),
                'range_margin_perc': fields.Float(required=True, description="Percentage margin of the value ranges"),
                'color': fields.String(required=True, description="Figure color"),
            }), required=True),
            'margin': fields.Nested(ns.model('BarStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False)
        }), required=True)
    })
