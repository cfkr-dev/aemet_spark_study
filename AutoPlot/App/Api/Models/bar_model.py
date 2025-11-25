import re

from typing import Optional
from pathlib import Path

from App.Utils.Storage.Core.storage import Storage
from App.Utils.validator import Validator
from App.Utils.file_utils import get_src_path, get_dest_path


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
        self.y = AxisComponent()

    def setup(self, data: dict):
        self.x.setup(data['x'])
        self.y.setup(data['y'])

    def validate(self, validator: Validator):
        self.x.validate(validator)
        self.y.validate(validator)


class Src:
    def __init__(self):
        self.path: Optional[Path] = None
        self.axis = Axis()

    def setup(self, data: dict):
        self.path = get_src_path(data['path'])
        self.axis.setup(data['axis'])

    def validate(self, validator: Validator):
        if not validator.storage.exists(self.path.as_posix()):
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

class BuildItem:
    def __init__(self):
        self.text_before = ""
        self.name = ""
        self.text_after = ""

    def setup(self, data: dict):
        self.text_before = data['text_before']
        self.name = data['name']
        self.text_after = data['text_after']

    def validate(self, validator: Validator):
        return


class InsideInfoItem:
    def __init__(self):
        self.label = ""
        self.build = []

    def setup(self, data: dict):
        self.label = data['label']

        for item in data['build']:
            new_item = BuildItem()
            new_item.setup(item)
            self.build.append(new_item)

    def validate(self, validator: Validator):
        if not len(self.build) > 0:
            validator.set_invalid()
            validator.add_error_msg('Build must contain at least one item')

        for item in self.build:
            item.validate(validator)


class Lettering:
    def __init__(self):
        self.title = ""
        self.subtitle = None
        self.x_label = ""
        self.y_label = ""
        self.inside_info = []

    def setup(self, data: dict):
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)
        self.x_label = data['x_label']
        self.y_label = data['y_label']

        if data.get('inside_info'):
            for item in data['inside_info']:
                new_item = InsideInfoItem()
                new_item.setup(item)
                self.inside_info.append(new_item)

    def validate(self, validator: Validator):
        for item in self.inside_info:
            item.validate(validator)


class Figure:
    def __init__(self):
        self.color = ""
        self.inverted_horizontal_axis = False
        self.threshold_limit_max_min = 0.0
        self.threshold_perc_limit_outside_text = 0.0
        self.range_margin_perc = 0.0

    def setup(self, data: dict):
        self.color = data['color']
        self.inverted_horizontal_axis = data['inverted_horizontal_axis']
        self.threshold_limit_max_min = data['threshold_limit_max_min']
        self.threshold_perc_limit_outside_text = data['threshold_perc_limit_outside_text']
        self.range_margin_perc = data['range_margin_perc']

    def validate(self, validator: Validator):
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')

        if 0 >= self.threshold_perc_limit_outside_text >= 1:
            validator.set_invalid()
            validator.add_error_msg('threshold_perc_limit_outside_text must be between 0 and 1')

        if 0 >= self.range_margin_perc >= 1:
            validator.set_invalid()
            validator.add_error_msg('range_margin_perc must be between 0 and 1')


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


class BarModel:
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
