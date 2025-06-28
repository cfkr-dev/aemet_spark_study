from flask_restx import fields, Namespace

from App.Config.constants import SPAIN_GEOGRAPHIC_LOCATIONS_LIST

"""

    {
        "src": {
            "path": "spark/temp/evol/cadiz/evol",
            "names": {
                "longitude" : "long_dec",
                "latitude" : "lat_dec",
                "value": ""
            },
            "location": "continental"
        },
        "dest" : {
            "path": "temp/evol/cadiz/evol",
            "filename": "plot_test",
            "export_png": true
        },
        "style": {
            "lettering": {
                "title": "TITLE TEST",
                "subtitle": "SUBTITLE TEST",
                "long_label": "Longitud",
                "lat_label": "Latitud",
                "legend_label": "Var (u)",
                "point_info": [
                    {
                        "label": "Station",
                        "build": [
                            {
                                "text_before": "",
                                "name": "station_id", 
                                "text_after": " "
                            },
                            {
                                "text_before": "",
                                "name": "station_name", 
                                "text_after": " "
                            },
                            {
                                "text_before": "(",
                                "name": "state", 
                                "text_after": ")."
                            }
                        ]
                    },
                    {
                        "label": "Location",
                        "build": [
                            {
                                "text_before": "",
                                "name": "lat_dms", 
                                "text_after": " "
                            },
                            {
                                "text_before": "",
                                "name": "long_dms", 
                                "text_after": " "
                            },
                            {
                                "text_before": "(",
                                "name": "altitude", 
                                "text_after": " m)."
                            }
                        ]
                    }
                ] 
            },
            "figure": {
                "name": "var",
                "color": "#000000",
                "color_opacity": 0.5,
                "point_size": 3
            },
            "margin": {
                "left": 120,
                "right": 120,
                "top": 100,
                "bottom": 100
            }
        }
    }


"""

class HeatMapDTO:
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    return ns.model('HeatMapInput', {
        'src': fields.Nested(ns.model('HeatMapSrc', {
            'path': fields.String(required=True, description="Relative route to data"),
            'names': fields.Nested(ns.model('HeatMapSrcNames', {
                'longitude': fields.String(required=True, description="Longitude column name"),
                'latitude': fields.String(required=True, description="Latitude column name"),
                'value': fields.String(required=True, description="Value column name"),
            }), required=True),
            'location': fields.String(required=True, description="Spain geographic location", enum=SPAIN_GEOGRAPHIC_LOCATIONS_LIST),
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
