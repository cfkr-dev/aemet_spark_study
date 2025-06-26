import re
from pathlib import Path
from app.Config.Enumerations import *
from app.Utils.Validator import Validator
from app.Utils.FileUtils import get_src_path, get_dest_path


class AxisComponent:
    def __init__(self):
        self.name = ""

    def setup(self, data: dict):
        self.name = data['name']

    def validate(self, validator: Validator):
        return


class Axis:
    def __init__(self):
        self.x = AxisComponent()
        self.y_temp = AxisComponent()
        self.y_prec = AxisComponent()

    def setup(self, data: dict):
        self.x.setup(data['x'])
        self.y_temp.setup(data['y_temp'])
        self.y_prec.setup(data['y_prec'])

    def validate(self, validator: Validator):
        self.x.validate(validator)
        self.y_temp.validate(validator)
        self.y_prec.validate(validator)


class Src:
    def __init__(self):
        self.path: Optional[Path] = None
        self.axis = Axis()

    def setup(self, data: dict):
        self.path = get_src_path(data['path'])
        self.axis.setup(data['axis'])

    def validate(self, validator: Validator):
        if not self.path.exists():
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        self.axis.validate(validator)


class Dest:
    def __init__(self):
        self.path: Optional[Path] = None
        self.filename = ""
        self.export_png = True

    def setup(self, data: dict):
        self.path = get_dest_path(data['path'])
        self.filename = data['filename']
        self.export_png = data['export_png']

    def validate(self, validator: Validator):
        return


class Lettering:
    def __init__(self):
        self.title = ""
        self.subtitle = None
        self.x_label = ""
        self.y_temp_label = ""
        self.y_prec_label = ""

    def setup(self, data: dict):
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)
        self.x_label = data['x_label']
        self.y_temp_label = data['y_temp_label']
        self.y_prec_label = data['y_prec_label']

    def validate(self, validator: Validator):
        return


class Figure:
    def __init__(self):
        self.name = ""
        self.color = ""

    def setup(self, data: dict):
        self.name = data['name']
        self.color = data['color']

    def validate(self, validator: Validator):
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')


class Margin:
    def __init__(self):
        self.left = 0.0
        self.right = 0.0
        self.top = 0.0
        self.bottom = 0.0

    def setup(self, data: dict):
        self.left = data.get('left', 80.0)
        self.right = data.get('right', 80.0)
        self.top = data.get('top', 80.0)
        self.bottom = data.get('bottom', 80.0)

    def validate(self, validator: Validator):
        return


class Legend:
    def __init__(self):
        self.show_legend = True
        self.y_offset = 0.0

    def setup(self, data: dict):
        self.show_legend = True if data.get('y_offset') else False
        self.y_offset = data.get('y_offset', -0.3)

    def validate(self, validator: Validator):
        return


class Style:
    def __init__(self):
        self.lettering = Lettering()
        self.figure_temp = Figure()
        self.figure_prec = Figure()
        self.margin = Margin()
        self.legend = Legend()

    def setup(self, data: dict):
        self.lettering.setup(data['lettering'])
        self.figure_temp.setup(data['figure_temp'])
        self.figure_prec.setup(data['figure_prec'])
        self.margin.setup(data.get('margin', {}))
        self.legend.setup(data.get('legend', {}))

    def validate(self, validator: Validator):
        self.lettering.validate(validator)
        self.figure_temp.validate(validator)
        self.figure_prec.validate(validator)
        self.margin.validate(validator)
        self.legend.validate(validator)


class ClimographModel:
    def __init__(self):
        self.src = Src()
        self.dest = Dest()
        self.style = Style()

    def setup(self, data: dict):
        self.src.setup(data['src'])
        self.dest.setup(data['dest'])
        self.style.setup(data['style'])

    def validate(self):
        validator = Validator()

        self.src.validate(validator)

        self.dest.validate(validator)

        self.style.validate(validator)

        return validator
