"""
DTOs for Pie endpoint.

Defines request payload model (flask-restx) for the pie API.

.. module:: App.Api.DTOS.pie_dto
"""

from flask_restx import fields, Namespace


class PieDTO:
    """Container for Pie endpoint request model.

    Exposes `post_input` which is a flask-restx model describing the expected JSON
    payload for POST requests to the pie endpoint.

    :param ns: The flask-restx Namespace used to register models and routes.
    :type ns: flask_restx.Namespace
    """
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    """Create and return the flask-restx model describing the POST input payload.

    :param ns: The flask-restx Namespace used to register the model.
    :type ns: flask_restx.Namespace
    :returns: A flask-restx model describing the pie POST payload.
    """
    return ns.model('PieInput', {
        'src': fields.Nested(ns.model('PieSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            'names': fields.Nested(ns.model('PieSrcNames', {
                'lower_bound': fields.String(reqired=True, description="Set lower bound column name"),
                'upper_bound': fields.String(reqired=True, description="Set upper bound column name"),
                'value': fields.String(reqired=True, description="Set value column name"),
            }), required=True)
        })),
        'dest': fields.Nested(ns.model('PieDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('PieStyle', {
            'lettering': fields.Nested(ns.model('PieStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart")
            }), required=True),
            'margin': fields.Nested(ns.model('PieStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False),
            'show_legend': fields.Boolean(required=True, description="Show legend"),
        }), required=True)
    })
