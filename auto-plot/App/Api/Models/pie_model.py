"""Pie chart model containers and validators.

.. module:: App.Api.Models.pie_model

This module defines small configuration containers used to parse and
validate pie chart configuration dictionaries.
"""

from pathlib import Path

from App.Config.enumerations import *
from App.Utils.Storage.Core.storage import Storage
from App.Utils.file_utils import get_src_path, get_dest_path
from App.Utils.validator import Validator


class Names:
    """Column name mappings required by the pie chart.

    :ivar lower_bound: Lower bound column name.
    :type lower_bound: str
    :ivar upper_bound: Upper bound column name.
    :type upper_bound: str
    :ivar value: Value column name.
    :type value: str
    """

    def __init__(self):
        """Initialize :class:`Names` with empty column names."""
        self.lower_bound = ""
        self.upper_bound = ""
        self.value = ""

    def setup(self, data: dict):
        """Populate names from mapping.

        :param data: Mapping with keys ``'lower_bound'``, ``'upper_bound'`` and ``'value'``.
        :type data: dict
        """
        self.lower_bound = data['lower_bound']
        self.upper_bound = data['upper_bound']
        self.value = data['value']

    def validate(self, validator: Validator):
        """No-op names validation."""
        return


class Src:
    """Source configuration for the pie chart."""

    def __init__(self):
        """Create an empty :class:`Src` with default path and names."""
        self.path: Optional[Path] = None
        self.names = Names()

    def setup(self, data: dict):
        """Populate source settings from mapping.

        :param data: Mapping with keys ``'path'`` and ``'names'``.
        :type data: dict
        """
        self.path = get_src_path(data['path'])
        self.names.setup(data['names'])

    def validate(self, validator: Validator):
        """Validate the source and its names mapping.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        self.names.validate(validator)


class Dest:
    """Destination configuration for pie chart output."""

    def __init__(self):
        """Initialize :class:`Dest` defaults for pie outputs."""
        self.path: Optional[Path] = None
        self.filename = ""
        self.export_png = True

    def setup(self, data: dict):
        """Populate destination from mapping.

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
    """Short textual configuration for pie charts (title and subtitle)."""

    def __init__(self):
        """Create default lettering values for a pie chart."""
        self.title = ""
        self.subtitle = None

    def setup(self, data: dict):
        """Populate lettering from mapping.

        :param data: Mapping with keys ``'title'`` and optional ``'subtitle'``.
        :type data: dict
        """
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)

    def validate(self, validator: Validator):
        """No-op lettering validation."""
        return


class Margin:
    """Margins container with sensible defaults."""

    def __init__(self):
        """Initialize margin defaults for pie charts."""
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
        """No-op margin validation."""
        return


class Style:
    """Style configuration including lettering, margins and legend toggle."""

    def __init__(self):
        """Create a :class:`Style` with default lettering and margin."""
        self.lettering = Lettering()
        self.margin = Margin()
        self.show_legend = True

    def setup(self, data: dict):
        """Populate style from mapping.

        :param data: Mapping with keys ``'lettering'``, optional ``'margin'`` and ``'show_legend'``.
        :type data: dict
        """
        self.lettering.setup(data['lettering'])
        self.margin.setup(data.get('margin', {}))
        self.show_legend = data['show_legend']

    def validate(self, validator: Validator):
        """Validate style subtree."""
        self.lettering.validate(validator)
        self.margin.validate(validator)


class PieModel:
    """Top-level pie chart configuration model used by controllers."""

    def __init__(self, storage: Storage):
        """Create a new PieModel.

        :param storage: Storage backend used by validation routines.
        :type storage: App.Utils.Storage.Core.storage.Storage
        """
        self.storage = storage
        self.src = Src()
        self.dest = Dest()
        self.style = Style()

    def setup(self, data: dict):
        """Populate the pie model from mapping.

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
