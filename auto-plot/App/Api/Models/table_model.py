import re

from pathlib import Path

from App.Config.enumerations import *
from App.Utils.Storage.Core.storage import Storage
from App.Utils.file_utils import get_src_path, get_dest_path
from App.Utils.validator import Validator


class Src:
    def __init__(self):
        self.path: Optional[Path] = None
        self.col_names = []

    def setup(self, data: dict):
        self.path = get_src_path(data['path'])

        for item in data['col_names']:
            self.col_names.append(item)

    def validate(self, validator: Validator):
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        validator.add_custom_data('col_names_len', len(self.col_names))


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
        self.headers = []

    def setup(self, data: dict):
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)

        for item in data['headers']:
            self.headers.append(item)

    def validate(self, validator: Validator):
        if len(self.headers) != validator.get_custom_data('col_names_len'):
            validator.set_invalid()
            validator.add_error_msg('Headers and column names must have the same length')


class TableFigureDefinition:
    def __init__(self):
        self.align = None
        self.color = ""

    def setup(self, data: dict):
        self.align = get_enum_value(data['align'], TableAligns)
        self.color = data['color']

    def validate(self, validator: Validator):
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')


class Figure:
    def __init__(self):
        self.headers = TableFigureDefinition()
        self.cells = TableFigureDefinition()

    def setup(self, data: dict):
        self.headers.setup(data['headers'])
        self.cells.setup(data['cells'])

    def validate(self, validator: Validator):
        self.headers.validate(validator)
        self.cells.validate(validator)


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
        self.figure = Figure()
        self.margin = Margin()

    def setup(self, data: dict):
        self.lettering.setup(data['lettering'])
        self.figure.setup(data['figure'])
        self.margin.setup(data.get('margin', {}))

    def validate(self, validator: Validator):
        self.lettering.validate(validator)
        self.figure.validate(validator)
        self.margin.validate(validator)


class TableModel:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.src = Src()
        self.dest = Dest()
        self.style = Style()

    def setup(self, data: dict):
        self.src.setup(data['src'])
        self.dest.setup(data['dest'])
        self.style.setup(data['style'])

    def validate(self):
        validator = Validator(self.storage)

        self.src.validate(validator)

        self.dest.validate(validator)

        self.style.validate(validator)

        return validator
