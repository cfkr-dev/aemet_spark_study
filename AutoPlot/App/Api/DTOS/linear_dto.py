from flask_restx import fields, Namespace

from App.Config.constants import FORMATTERS_LIST

"""

    {
        "src": {
            "path": "spark/temp/evol/cadiz/evol",
            "axis": {
                "x": {
                    "name": "date",
                    "format": "timestamp"
                },
                "y": {
                    "name": "temp_daily_avg"
                }
            }
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
                "x_label": "Date",
                "y_label": "Var (u)"
            },
            "figure": {
                "name": "var",
                "color": "#4169e1"
            },
            "margin": {
                "left": 120,
                "right": 120,
                "top": 100,
                "bottom": 100
            },
            "legend": {
                "y_offset": -0.05
            }
        }
    }


"""

class LinearDTO:
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
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
