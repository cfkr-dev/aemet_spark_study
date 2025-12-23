"""
DTOs for Table endpoint.

Defines the request payload model (flask-restx) expected by the table API.

.. module:: App.Api.DTOS.table_dto
"""

from flask_restx import fields, Namespace

from App.Config.constants import TABLE_ALIGNS_LIST


class TableDTO:
    """Container for the Table endpoint request model.

    The object exposes `post_input` which is a flask-restx model describing the
    expected JSON payload for POST requests to the table endpoint.

    :param ns: The flask-restx `Namespace` used to register models and routes.
    :type ns: flask_restx.Namespace
    """
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    """Create and return the flask-restx model describing the POST input payload.

    The model is built using nested `fields` definitions and mirrors the expected
    JSON structure consumed by the table endpoint.

    :param ns: The flask-restx Namespace used to build and register the model.
    :type ns: flask_restx.Namespace
    :returns: A flask-restx model describing the table POST payload.
    """
    return ns.model('TableInput', {
        'src': fields.Nested(ns.model('TableSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            "col_names": fields.List(fields.String, required=True, description="List of column names"),
        })),
        'dest': fields.Nested(ns.model('TableDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('TableStyle', {
            'lettering': fields.Nested(ns.model('TableStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart"),
                "headers": fields.List(fields.String, required=True, description="List of header titles"),
            }), required=True),
            'figure': fields.Nested(ns.model('TableFigure', {
                'headers': fields.Nested(ns.model('TableFigureHeaders', {
                    'align': fields.String(required=True, description="Headers align", enum=TABLE_ALIGNS_LIST),
                    'color': fields.String(required=True, description="Headers color"),
                }), required=True),
                'cells': fields.Nested(ns.model('TableFigureCells', {
                    'align': fields.String(required=True, description="Cells align", enum=TABLE_ALIGNS_LIST),
                    'color': fields.String(required=True, description="Cells color"),
                }), required=True),
            }), required=True),
            'margin': fields.Nested(ns.model('TableStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False)
        }), required=True)
    })
