import re

from pathlib import Path

from App.Config.enumerations import *
from App.Utils.file_utils import get_src_path, get_dest_path
from App.Utils.validator import Validator


class Names:
    def __init__(self):
        self.longitude = ""
        self.latitude = ""
        self.value = ""

    def setup(self, data: dict):
        self.longitude = data['longitude']
        self.latitude = data['latitude']
        self.value = data['value']

    def validate(self, validator: Validator):
        return


class Src:
    def __init__(self):
        self.path: Optional[Path] = None
        self.names = Names()
        self.location = None

    def setup(self, data: dict):
        self.path = get_src_path(data['path'])
        self.names.setup(data['names'])
        self.location = get_enum_value(data['location'], SpainGeographicLocations)

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


class PointInfoItem:
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
        self.long_label = ""
        self.lat_label = ""
        self.legend_label = ""
        self.point_info = []

    def setup(self, data: dict):
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)
        self.long_label = data['long_label']
        self.lat_label = data['lat_label']
        self.legend_label = data['legend_label']

        if data.get('point_info'):
            for item in data['point_info']:
                new_item = PointInfoItem()
                new_item.setup(item)
                self.point_info.append(new_item)

    def validate(self, validator: Validator):
        for item in self.point_info:
            item.validate(validator)


class Figure:
    def __init__(self):
        self.name = ""
        self.color = ""
        self.color_opacity = 0.0
        self.point_size = 0.0

    def setup(self, data: dict):
        self.name = data['name']
        self.color = data['color']
        self.color_opacity = data['color_opacity']
        self.point_size = data['point_size']

    def validate(self, validator: Validator):
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')

        if 0.0 >= self.color_opacity >= 1.0:
            validator.set_invalid()
            validator.add_error_msg('color_opacity must be between 0 and 1')

        if self.point_size <= 0.0:
            validator.set_invalid()
            validator.add_error_msg('point_size must be greater than 0')


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


class HeatMapModel:
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
