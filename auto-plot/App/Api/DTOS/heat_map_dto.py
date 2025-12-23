"""
DTOs for Heat Map endpoint.

Defines the request payload model (flask-restx) expected by the heat-map API.

.. module:: App.Api.DTOS.heat_map_dto
"""

from flask_restx import fields, Namespace

from App.Config.constants import SPAIN_GEOGRAPHIC_LOCATIONS_LIST


class HeatMapDTO:
    """Container for the HeatMap endpoint request model.

    Exposes `post_input` which is a flask-restx model describing the expected JSON
    payload for POST requests to the heat-map endpoint.

    :param ns: The flask-restx Namespace used to register models and routes.
    :type ns: flask_restx.Namespace
    """
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    """Create and return the flask-restx model describing the POST input payload.

    :param ns: The flask-restx Namespace used to register the model.
    :type ns: flask_restx.Namespace
    :returns: A flask-restx model describing the heat-map POST payload.
    """
    return ns.model('HeatMapInput', {
        'src': fields.Nested(ns.model('HeatMapSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            'names': fields.Nested(ns.model('HeatMapSrcNames', {
                'longitude': fields.String(required=True, description="Longitude column name"),
                'latitude': fields.String(required=True, description="Latitude column name"),
                'value': fields.String(required=True, description="Value column name"),
            }), required=True),
            'location': fields.String(required=True, description="Spain geographic location",
                                      enum=SPAIN_GEOGRAPHIC_LOCATIONS_LIST),
        })),
        'dest': fields.Nested(ns.model('HeatMapDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('HeatMapStyle', {
            'lettering': fields.Nested(ns.model('HeatMapStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart"),
                'long_label': fields.String(required=True, description="Longitude axis label"),
                'lat_label': fields.String(required=True, description="Latitude axis label"),
                'legend_label': fields.String(required=True, description="Legend label"),
                "point_info": fields.List(fields.Nested(ns.model('HeatMapStyleLetteringInsideInfoItem', {
                    'label': fields.String(required=True, description="Label for information"),
                    'build': fields.List(fields.Nested(ns.model('HeatMapStyleLetteringInsideInfoItemBuildItem', {
                        'text_before': fields.String(required=True, description="Text before"),
                        'name': fields.String(required=True, description="Name of the column"),
                        'text_after': fields.String(required=True, description="Text after"),
                    })), required=True)
                })), required=False),
            }), required=True),
            'figure': fields.Nested(ns.model('HeatMapStyleFigure', {
                'name': fields.String(required=True, description="Figure name"),
                'color': fields.String(required=True, description="Figure color"),
                'color_opacity': fields.Float(required=True, description="Figure color opacity"),
                'point_size': fields.Float(required=True, description="Figure points size"),
            }), required=True),
            'margin': fields.Nested(ns.model('HeatMapStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False)
        }), required=True)
    })
