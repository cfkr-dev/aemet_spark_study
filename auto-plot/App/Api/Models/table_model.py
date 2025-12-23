"""Table model containers and validation helpers.

.. module:: App.Api.Models.table_model

Defines configuration containers for table plots, including column names,
headers and visual definitions for headers and cells.
"""

import re

from pathlib import Path

from App.Config.enumerations import *
from App.Utils.Storage.Core.storage import Storage
from App.Utils.file_utils import get_src_path, get_dest_path
from App.Utils.validator import Validator


class Src:
    """Source specification for table data.

    :ivar path: Resolved source path.
    :type path: pathlib.Path | None
    :ivar col_names: Column names read from the configuration.
    :type col_names: list[str]
    """

    def __init__(self):
        """Create an empty :class:`Src` with defaults for path and column names."""
        self.path: Optional[Path] = None
        self.col_names = []

    def setup(self, data: dict):
        """Populate the source settings from configuration mapping.

        :param data: Mapping expected to contain ``'path'`` and ``'col_names'``.
        :type data: dict
        """
        self.path = get_src_path(data['path'])

        for item in data['col_names']:
            self.col_names.append(item)

    def validate(self, validator: Validator):
        """Validate source path and register column names length.

        The method sets the validator as invalid if the path does not exist
        and stores a custom value ``'col_names_len'`` used by other
        validators.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        validator.add_custom_data('col_names_len', len(self.col_names))


class Dest:
    """Destination configuration for table output file."""

    def __init__(self):
        """Initialize :class:`Dest` with default output settings."""
        self.path: Optional[Path] = None
        self.filename = ""
        self.export_png = True

    def setup(self, data: dict):
        """Populate destination settings from mapping.

        :param data: Mapping that must include ``'path'``, ``'filename'`` and ``'export_png'``.
        :type data: dict
        """
        self.path = get_dest_path(data['path'])
        self.filename = data['filename']
        self.export_png = data['export_png']

    def validate(self, validator: Validator):
        """No-op destination validation."""
        return


class Lettering:
    """Title and headers used in table rendering."""

    def __init__(self):
        """Create default lettering and header list."""
        self.title = ""
        self.subtitle = None
        self.headers = []

    def setup(self, data: dict):
        """Populate lettering and headers from mapping.

        :param data: Mapping containing ``'title'`` and ``'headers'``.
        :type data: dict
        """
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)

        for item in data['headers']:
            self.headers.append(item)

    def validate(self, validator: Validator):
        """Ensure headers count matches source column names.

        :param validator: Validator used to access custom data.
        :type validator: App.Utils.validator.Validator
        """
        if len(self.headers) != validator.get_custom_data('col_names_len'):
            validator.set_invalid()
            validator.add_error_msg('Headers and column names must have the same length')


class TableFigureDefinition:
    """Visual definition for table headers/cells (alignment and color)."""

    def __init__(self):
        """Create a :class:`TableFigureDefinition` with default values."""
        self.align = None
        self.color = ""

    def setup(self, data: dict):
        """Populate table figure definition from mapping.

        :param data: Mapping with keys ``'align'`` and ``'color'``.
        :type data: dict
        """
        self.align = get_enum_value(data['align'], TableAligns)
        self.color = data['color']

    def validate(self, validator: Validator):
        """Validate color format.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')


class Figure:
    """Grouping of header and cell visual definitions."""

    def __init__(self):
        """Create :class:`Figure` grouping header and cell definitions."""
        self.headers = TableFigureDefinition()
        self.cells = TableFigureDefinition()

    def setup(self, data: dict):
        """Populate figure definitions from mapping.

        :param data: Mapping with keys ``'headers'`` and ``'cells'``.
        :type data: dict
        """
        self.headers.setup(data['headers'])
        self.cells.setup(data['cells'])

    def validate(self, validator: Validator):
        """Validate header and cell figures.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        self.headers.validate(validator)
        self.cells.validate(validator)


class Margin:
    def __init__(self):
        """Create default margin values for table layout."""
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
        """No-op validation for margins.

        :param validator: Validator to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class Style:
    def __init__(self):
        """Create the style container with default children."""
        self.lettering = Lettering()
        self.figure = Figure()
        self.margin = Margin()

    def setup(self, data: dict):
        """Populate the style from mapping.

        :param data: Mapping containing keys ``'lettering'``, ``'figure'`` and optional
                     ``'margin'``.
        :type data: dict
        """
        self.lettering.setup(data['lettering'])
        self.figure.setup(data['figure'])
        self.margin.setup(data.get('margin', {}))

    def validate(self, validator: Validator):
        """Validate the style subtree by delegating to its children.

        :param validator: Validator used to collect validation messages.
        :type validator: App.Utils.validator.Validator
        """
        self.lettering.validate(validator)
        self.figure.validate(validator)
        self.margin.validate(validator)


class TableModel:
    """Model representing the configuration for a table plot."""

    def __init__(self, storage: Storage):
        """Create a new TableModel.

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
        """Validate model and return a :class:`Validator` instance."""
        validator = Validator(self.storage)

        self.src.validate(validator)

        self.dest.validate(validator)

        self.style.validate(validator)

        return validator
