from pathlib import Path

from app.Config.Enumerations import *
from app.Utils.FileUtils import get_src_path, get_dest_path
from app.Utils.Validator import Validator


class Names:
    def __init__(self):
        self.lower_bound = ""
        self.upper_bound = ""
        self.value = ""

    def setup(self, data: dict):
        self.lower_bound = data['lower_bound']
        self.upper_bound = data['upper_bound']
        self.value = data['value']

    def validate(self, validator: Validator):
        return


class Src:
    def __init__(self):
        self.path: Optional[Path] = None
        self.names = Names()

    def setup(self, data: dict):
        self.path = get_src_path(data['path'])
        self.names.setup(data['names'])

    def validate(self, validator: Validator):
        if not self.path.exists():
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        self.names.validate(validator)


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

    def setup(self, data: dict):
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)

    def validate(self, validator: Validator):
        return


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


class Style:
    def __init__(self):
        self.lettering = Lettering()
        self.margin = Margin()
        self.show_legend = True

    def setup(self, data: dict):
        self.lettering.setup(data['lettering'])
        self.margin.setup(data.get('margin', {}))
        self.show_legend = data['show_legend']

    def validate(self, validator: Validator):
        self.lettering.validate(validator)
        self.margin.validate(validator)


class PieModel:
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
