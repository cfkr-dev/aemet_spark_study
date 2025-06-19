from flask_restx import fields, Namespace

from Config.constants import FORMATTERS_LIST

"""

    {
        "src": {
            "main": {
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
            "regression": {
                "path": "spark/temp/evol/cadiz/evol",
                "names": {
                    "slope": "beta_1",
                    "intercept": "beta_0"
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
                "y_1_label": "Var (u)",
                "y_2_label": "Var (u)"
            },
            "figure_1": {
                "name": "var",
                "color": "#4169e1"
            },
            "figure_2": {
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

class LinearRegressionDTO:
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
    return ns.model('LinearRegressionInput', {
        'src': fields.Nested(ns.model('LinearRegressionSrc', {
            'main': fields.Nested(ns.model('LinearRegressionSrcMain', {
                'path': fields.String(required=True, description="Relative route to main data"),
                'axis': fields.Nested(ns.model('LinearRegressionSrcAxis', {
                    'x': fields.Nested(ns.model('LinearRegressionSrcAxisX', {
                        'name': fields.String(reqired=True, description="X Column name"),
                        'format': fields.String(reqired=False, description="Column format", enum=FORMATTERS_LIST),
                    }), required=True),
                    'y': fields.Nested(ns.model('LinearRegressionSrcAxisY', {
                        'name': fields.String(reqired=True, description="Y Column name")
                    }), required=True)
                }), required=True)
            }), required=True),
            'regression': fields.Nested(ns.model('LinearRegressionSrcRegression', {
                'path': fields.String(required=True, description="Relative route to regression data"),
                'names': fields.Nested(ns.model('LinearRegressionSrcRegressionNames', {
                    'slope': fields.String(required=True, description="Slope Column name"),
                    'intercept': fields.String(required=True, description="Intercept Column name"),
                }), required=True)
            }), required=True),
        })),
        'dest': fields.Nested(ns.model('LinearRegressionDest', {
            'path': fields.String(required=True, description="Relative output route"),
            'filename': fields.String(required=True, description="Filename of the chart"),
            'export_png': fields.Boolean(required=True, description="Export png"),
        }), required=True),
        'style': fields.Nested(ns.model('LinearRegressionStyle', {
            'lettering': fields.Nested(ns.model('LinearRegressionStyleLettering', {
                'title': fields.String(required=True, description="Title of the chart"),
                'subtitle': fields.String(required=False, description="Subtitle of the chart"),
                'x_label': fields.String(required=True, description="X axis label"),
                'y_1_label': fields.String(required=True, description="Y 1 axis label"),
                'y_2_label': fields.String(required=True, description="Y 2 axis label"),
            }), required=True),
            'figure_1': fields.Nested(ns.model('LinearRegressionStyleFigure1', {
                'name': fields.String(required=True, description="Figure 1 name"),
                'color': fields.String(required=True, description="Figure 1 color"),
            }), required=True),
            'figure_2': fields.Nested(ns.model('LinearRegressionStyleFigure2', {
                'name': fields.String(required=True, description="Figure 2 name"),
                'color': fields.String(required=True, description="Figure 2 color"),
            }), required=True),
            'margin': fields.Nested(ns.model('LinearRegressionStyleMargin', {
                'left': fields.Float(required=True, description="Margin left"),
                'right': fields.Float(required=True, description="Margin right"),
                'top': fields.Float(required=True, description="Margin top"),
                'bottom': fields.Float(required=True, description="Margin bottom")
            }), required=False),
            'legend': fields.Nested(ns.model('LinearRegressionStyleLegend', {
                'y_offset': fields.Float(required=True, description="Legend Y axis offset"),
            }), required=False),
        }), required=True)
    })
