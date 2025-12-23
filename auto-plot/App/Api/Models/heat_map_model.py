"""Heat map model definitions and configuration containers.

.. module:: App.Api.Models.heat_map_model

This module provides small containers that parse and validate configuration
used by heat map plotting routines. Each container exposes ``setup`` and
``validate`` methods; the top-level :class:`HeatMapModel` exposes
:meth:`setup` and :meth:`validate` to populate and validate the whole
configuration.
"""

import re

from pathlib import Path

from App.Config.enumerations import *
from App.Utils.Storage.Core.storage import Storage
from App.Utils.file_utils import get_src_path, get_dest_path
from App.Utils.validator import Validator


class Names:
    """Names mapping for longitude, latitude and value columns.

    :ivar longitude: Column name used for longitude values.
    :type longitude: str
    :ivar latitude: Column name used for latitude values.
    :type latitude: str
    :ivar value: Column name used for the measured value.
    :type value: str
    """

    def __init__(self):
        """Create an empty :class:`Names` container."""
        self.longitude = ""
        self.latitude = ""
        self.value = ""

    def setup(self, data: dict):
        """Populate the names mapping from configuration.

        :param data: Mapping with keys ``'longitude'``, ``'latitude'`` and ``'value'``.
        :type data: dict
        :raises KeyError: If any required key is missing.
        """
        self.longitude = data['longitude']
        self.latitude = data['latitude']
        self.value = data['value']

    def validate(self, validator: Validator):
        """Validate the names mapping (no-op currently).

        :param validator: Validator used to collect issues.
        :type validator: App.Utils.validator.Validator
        """
        return


class Src:
    """Source configuration including path, column names and geographic location.

    :ivar path: Resolved path to the source file.
    :type path: pathlib.Path | None
    :ivar names: Column name mapping.
    :type names: Names
    :ivar location: Resolved geographic location enum value.
    :type location: object | None
    """

    def __init__(self):
        """Create an empty :class:`Src` with default children."""
        self.path: Optional[Path] = None
        self.names = Names()
        self.location = None

    def setup(self, data: dict):
        """Populate the source configuration from mapping.

        :param data: Mapping expected to contain ``'path'``, ``'names'`` and
                     ``'location'`` keys.
        :type data: dict
        :raises KeyError: If a required key is missing.
        """
        self.path = get_src_path(data['path'])
        self.names.setup(data['names'])
        self.location = get_enum_value(data['location'], SpainGeographicLocations)

    def validate(self, validator: Validator):
        """Validate the source configuration.

        Checks that the resolved source path exists in the provided storage
        and delegates further checks to :class:`Names`.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        self.names.validate(validator)


class Dest:
    """Destination settings for heat map outputs.

    :ivar path: Resolved destination directory.
    :type path: pathlib.Path | None
    :ivar filename: Output filename.
    :type filename: str
    :ivar export_png: Whether to export an additional PNG artifact.
    :type export_png: bool
    """

    def __init__(self):
        """Create an empty :class:`Dest` with default values."""
        self.path: Optional[Path] = None
        self.filename = ""
        self.export_png = True

    def setup(self, data: dict):
        """Populate destination configuration from mapping.

        :param data: Mapping with keys ``'path'``, ``'filename'`` and ``'export_png'``.
        :type data: dict
        """
        self.path = get_dest_path(data['path'])
        self.filename = data['filename']
        self.export_png = data['export_png']

    def validate(self, validator: Validator):
        """Validate destination settings (no-op).

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class BuildItem:
    """Building block used to compose point-information text.

    :ivar text_before: Text displayed before the value.
    :type text_before: str
    :ivar name: Field name to extract from the source row.
    :type name: str
    :ivar text_after: Text displayed after the value.
    :type text_after: str
    """

    def __init__(self):
        """Create an empty :class:`BuildItem`."""
        self.text_before = ""
        self.name = ""
        self.text_after = ""

    def setup(self, data: dict):
        """Populate the build item from mapping.

        :param data: Mapping with keys ``'text_before'``, ``'name'`` and
                     ``'text_after'``.
        :type data: dict
        """
        self.text_before = data['text_before']
        self.name = data['name']
        self.text_after = data['text_after']

    def validate(self, validator: Validator):
        """No-op validation for the build item."""
        return


class PointInfoItem:
    """Group of build items used to render point information tooltips.

    :ivar label: Label for the point-info group.
    :type label: str
    :ivar build: Sequence of :class:`BuildItem` used to compose the text.
    :type build: list[BuildItem]
    """

    def __init__(self):
        """Create an empty :class:`PointInfoItem`."""
        self.label = ""
        self.build = []

    def setup(self, data: dict):
        """Populate the point-info item from mapping.

        :param data: Mapping with keys ``'label'`` and ``'build'``.
        :type data: dict
        """
        self.label = data['label']

        for item in data['build']:
            new_item = BuildItem()
            new_item.setup(item)
            self.build.append(new_item)

    def validate(self, validator: Validator):
        """Validate that the build list contains at least one item and validate each.

        :param validator: Validator collecting validation messages.
        :type validator: App.Utils.validator.Validator
        """
        if not len(self.build) > 0:
            validator.set_invalid()
            validator.add_error_msg('Build must contain at least one item')

        for item in self.build:
            item.validate(validator)


class Lettering:
    """Textual labels for heat map title, axes and legend.

    :ivar title: Chart title text.
    :type title: str
    :ivar subtitle: Optional subtitle text.
    :type subtitle: str | None
    :ivar long_label: Longitude axis label.
    :type long_label: str
    :ivar lat_label: Latitude axis label.
    :type lat_label: str
    :ivar legend_label: Legend label text.
    :type legend_label: str
    :ivar point_info: List of :class:`PointInfoItem` instances.
    :type point_info: list[PointInfoItem]
    """

    def __init__(self):
        """Create default lettering values (mostly empty strings)."""
        self.title = ""
        self.subtitle = None
        self.long_label = ""
        self.lat_label = ""
        self.legend_label = ""
        self.point_info = []

    def setup(self, data: dict):
        """Populate lettering from mapping.

        :param data: Mapping expected to contain title and axis labels; may
                     include ``'point_info'`` which should be an iterable of mappings.
        :type data: dict
        """
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
        """Validate configured point-info items by delegating to them.

        :param validator: Validator used to collect issues.
        :type validator: App.Utils.validator.Validator
        """
        for item in self.point_info:
            item.validate(validator)


class Figure:
    """Figure appearance settings such as color, opacity and point size.

    :ivar name: Figure name or identifier.
    :type name: str
    :ivar color: Hex color string used for points.
    :type color: str
    :ivar color_opacity: Opacity for the point color in range (0, 1).
    :type color_opacity: float
    :ivar point_size: Size of the points (must be > 0).
    :type point_size: float
    """

    def __init__(self):
        """Create a :class:`Figure` with default appearance values."""
        self.name = ""
        self.color = ""
        self.color_opacity = 0.0
        self.point_size = 0.0

    def setup(self, data: dict):
        """Populate figure appearance from mapping.

        :param data: Mapping with keys ``'name'``, ``'color'``, ``'color_opacity'``
                     and ``'point_size'``.
        :type data: dict
        """
        self.name = data['name']
        self.color = data['color']
        self.color_opacity = data['color_opacity']
        self.point_size = data['point_size']

    def validate(self, validator: Validator):
        """Validate figure appearance (color format, opacity and point size).

        :param validator: Validator used to collect validation messages.
        :type validator: App.Utils.validator.Validator
        """
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
    """Simple margins container with default values."""

    def __init__(self):
        """Create a :class:`Margin` initialized with default zeros."""
        self.left = 0.0
        self.right = 0.0
        self.top = 0.0
        self.bottom = 0.0

    def setup(self, data: dict):
        """Populate margin values from mapping.

        :param data: Optional mapping containing margin keys.
        :type data: dict
        """
        self.left = data.get('left', 80.0)
        self.right = data.get('right', 80.0)
        self.top = data.get('top', 80.0)
        self.bottom = data.get('bottom', 80.0)

    def validate(self, validator: Validator):
        """No validation performed for margins (kept for compatibility)."""
        return


class Style:
    """Top-level style container for heat map configuration.

    :ivar lettering: Lettering settings.
    :type lettering: Lettering
    :ivar figure: Figure settings.
    :type figure: Figure
    :ivar margin: Margin settings.
    :type margin: Margin
    """

    def __init__(self):
        """Create the default style container."""
        self.lettering = Lettering()
        self.figure = Figure()
        self.margin = Margin()

    def setup(self, data: dict):
        """Populate style from mapping.

        :param data: Mapping containing keys ``'lettering'``, ``'figure'`` and optional
                     ``'margin'``.
        :type data: dict
        """
        self.lettering.setup(data['lettering'])
        self.figure.setup(data['figure'])
        self.margin.setup(data.get('margin', {}))

    def validate(self, validator: Validator):
        """Validate the style subtree by delegating to its children.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        self.lettering.validate(validator)
        self.figure.validate(validator)
        self.margin.validate(validator)


class HeatMapModel:
    """Model representing heat map configuration used by controllers.

    This top-level model provides :meth:`setup` and :meth:`validate` to
    populate and validate the whole configuration structure.
    """

    def __init__(self, storage: Storage):
        """Create a new :class:`HeatMapModel`.

        :param storage: Storage backend used by validation routines.
        :type storage: App.Utils.Storage.Core.storage.Storage
        """
        self.storage = storage
        self.src = Src()
        self.dest = Dest()
        self.style = Style()

    def setup(self, data: dict):
        """Populate the model from a configuration mapping.

        :param data: Mapping containing top-level keys ``'src'``, ``'dest'`` and
                     ``'style'``.
        :type data: dict
        """
        self.src.setup(data['src'])
        self.dest.setup(data['dest'])
        self.style.setup(data['style'])

    def validate(self):
        """Validate the model and return a :class:`Validator` instance.

        The returned validator contains accumulated validation state and messages.

        :returns: Validator instance with validation results.
        :rtype: App.Utils.validator.Validator
        """
        validator = Validator(self.storage)

        self.src.validate(validator)

        self.dest.validate(validator)

        self.style.validate(validator)

        return validator
