"""Linear model definitions and configuration containers.

.. module:: App.Api.Models.linear_model

Provides containers used to parse and validate linear plot configuration
objects. The classes focus on configuration handling and validation.
"""

import re

from pathlib import Path

from App.Config.enumerations import *
from App.Utils.Storage.Core.storage import Storage
from App.Utils.file_utils import get_src_path, get_dest_path
from App.Utils.validator import Validator


class AxisComponent:
    """Axis component with optional formatter enum.

    :ivar name: Component name.
    :type name: str
    :ivar format: Resolved formatter enum or None.
    :type format: object | None
    """

    def __init__(self):
        """Create an empty :class:`AxisComponent`.

        Attributes default to empty values and should be set with :meth:`setup`.
        """
        self.name = ""
        self.format = None

    def setup(self, data: dict):
        """Populate component from mapping.

        :param data: Mapping expected to contain ``'name'`` and optionally ``'format'``.
        :type data: dict
        """
        self.name = data['name']
        self.format = get_enum_value(data.get('format'), Formatters)

    def validate(self, validator: Validator):
        """No-op validation for the axis component.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class Axis:
    """X/Y axis container for linear plots."""

    def __init__(self):
        """Initialize Axis with empty X and Y components."""
        self.x = AxisComponent()
        self.y = AxisComponent()

    def setup(self, data: dict):
        """Populate axis components from mapping.

        :param data: Mapping with keys ``'x'`` and ``'y'``.
        :type data: dict
        """
        self.x.setup(data['x'])
        self.y.setup(data['y'])

    def validate(self, validator: Validator):
        """Validate child axis components.

        :param validator: Validator used to collect messages.
        :type validator: App.Utils.validator.Validator
        """
        self.x.validate(validator)
        self.y.validate(validator)


class Src:
    """Source configuration including a path and axis mapping."""

    def __init__(self):
        """Create an empty :class:`Src` with default values."""
        self.path: Optional[Path] = None
        self.axis = Axis()

    def setup(self, data: dict):
        """Populate source settings from mapping.

        :param data: Mapping containing ``'path'`` and ``'axis'``.
        :type data: dict
        """
        self.path = get_src_path(data['path'])
        self.axis.setup(data['axis'])

    def validate(self, validator: Validator):
        """Validate that the source path exists and validate axis.

        :param validator: Validator to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        self.axis.validate(validator)


class Dest:
    """Destination configuration for linear plot outputs."""

    def __init__(self):
        """Initialize :class:`Dest` defaults."""
        self.path: Optional[Path] = None
        self.filename = ""
        self.export_png = True

    def setup(self, data: dict):
        """Populate destination settings from mapping.

        :param data: Mapping with keys ``'path'``, ``'filename'`` and ``'export_png'``.
        :type data: dict
        """
        self.path = get_dest_path(data['path'])
        self.filename = data['filename']
        self.export_png = data['export_png']

    def validate(self, validator: Validator):
        """No-op destination validation."""
        return


class Lettering:
    """Textual elements for linear plots (title and labels).

    :ivar title: Chart title.
    :type title: str
    :ivar subtitle: Optional subtitle.
    :type subtitle: str | None
    :ivar x_label: X axis label.
    :type x_label: str
    :ivar y_label: Y axis label.
    :type y_label: str
    """

    def __init__(self):
        """Create default lettering values."""
        self.title = ""
        self.subtitle = None
        self.x_label = ""
        self.y_label = ""

    def setup(self, data: dict):
        """Populate lettering from mapping.

        :param data: Mapping expected to contain title and axis labels.
        :type data: dict
        """
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)
        self.x_label = data['x_label']
        self.y_label = data['y_label']

    def validate(self, validator: Validator):
        """No-op validation for lettering."""
        return


class Figure:
    """Visual definition for linear figure elements."""

    def __init__(self):
        """Create an empty :class:`Figure`."""
        self.name = ""
        self.color = ""

    def setup(self, data: dict):
        """Populate figure from mapping.

        :param data: Mapping with keys ``'name'`` and ``'color'``.
        :type data: dict
        """
        self.name = data['name']
        self.color = data['color']

    def validate(self, validator: Validator):
        """Validate color format."""
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')


class Margin:
    def __init__(self):
        """Create default margin values."""
        self.left = 0.0
        self.right = 0.0
        self.top = 0.0
        self.bottom = 0.0

    def setup(self, data: dict):
        """Populate margin values from mapping.

        :param data: Mapping with optional margin keys.
        :type data: dict
        """
        self.left = data.get('left', 80.0)
        self.right = data.get('right', 80.0)
        self.top = data.get('top', 80.0)
        self.bottom = data.get('bottom', 80.0)

    def validate(self, validator: Validator):
        """No-op validation for margins."""
        return


class Legend:
    def __init__(self):
        """Initialize legend defaults."""
        self.show_legend = True
        self.y_offset = 0.0

    def setup(self, data: dict):
        """Populate legend settings from mapping.

        :param data: Mapping that may contain key ``'y_offset'``.
        :type data: dict
        """
        self.show_legend = True if data.get('y_offset') else False
        self.y_offset = data.get('y_offset', -0.3)

    def validate(self, validator: Validator):
        """No-op validation for legend."""
        return


class Style:
    def __init__(self):
        """Create a style container with default children."""
        self.lettering = Lettering()
        self.figure = Figure()
        self.margin = Margin()
        self.legend = Legend()

    def setup(self, data: dict):
        """Populate the style from a mapping.

        :param data: Mapping containing style sub-mappings.
        :type data: dict
        """
        self.lettering.setup(data['lettering'])
        self.figure.setup(data['figure'])
        self.margin.setup(data.get('margin', {}))
        self.legend.setup(data.get('legend', {}))

    def validate(self, validator: Validator):
        """Validate the style subtree by delegating to children.

        :param validator: Validator used to collect issues.
        :type validator: App.Utils.validator.Validator
        """
        self.lettering.validate(validator)
        self.figure.validate(validator)
        self.margin.validate(validator)
        self.legend.validate(validator)


class LinearModel:
    """Model representing a linear plot configuration used by controllers."""

    def __init__(self, storage: Storage):
        """Create a new LinearModel.

        :param storage: Storage backend used by validation routines.
        :type storage: App.Utils.Storage.Core.storage.Storage
        """
        self.storage = storage
        self.src = Src()
        self.dest = Dest()
        self.style = Style()

    def setup(self, data: dict):
        """Populate the model from a configuration mapping.

        :param data: Mapping containing ``'src'``, ``'dest'`` and ``'style'``.
        :type data: dict
        """
        self.src.setup(data['src'])
        self.dest.setup(data['dest'])
        self.style.setup(data['style'])

    def validate(self):
        """Validate the model and return a :class:`Validator` instance."""
        validator = Validator(self.storage)

        self.src.validate(validator)

        self.dest.validate(validator)

        self.style.validate(validator)

        return validator
