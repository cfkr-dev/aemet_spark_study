from flask_restx import fields, Namespace

from Config.constants import TABLE_ALIGNS_LIST

"""

    {
        "src": {
            "path": "spark/temp/evol/cadiz/evol",
            "col_names": ["top", "state"]
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
                "headers": ["Top", "Comunidad Autónoma"]
            },
            "figure": {
                "headers": {
                    "align": "center",
                    "color": "#4169e1"
                },
                "cells": {
                    "align": "center",
                    "color": "#4169e1"
                }
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


class TableDTO:
    def __init__(self, ns: Namespace):
        self.post_input = _create_input_post_dto(ns)


def _create_input_post_dto(ns: Namespace):
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
