"""Climograph model definitions and configuration containers.

.. module:: App.Api.Models.climograph_model

This module provides classes to parse and validate configuration for
climograph plots (temperature and precipitation). Each class implements
``setup`` and ``validate`` methods used by controllers.
"""

import re

from typing import Optional
from pathlib import Path

from App.Utils.Storage.Core.storage import Storage
from App.Utils.validator import Validator
from App.Utils.file_utils import get_src_path, get_dest_path


class AxisComponent:
    """Represents a named axis component.

    :ivar name: Component display name.
    :type name: str
    """

    def __init__(self):
        """Create an empty axis component."""
        self.name = ""

    def setup(self, data: dict):
        """Populate the instance from ``data``.

        :param data: Mapping that must contain the key ``'name'``.
        :type data: dict
        :raises KeyError: If the required key is missing.
        """
        self.name = data['name']

    def validate(self, validator: Validator):
        """Placeholder validation method (no-op).

        :param validator: Validator collecting validation messages.
        :type validator: App.Utils.validator.Validator
        """
        return


class Axis:
    """Container for climograph axes (x, y_temp, y_prec).

    :ivar x: X axis component.
    :type x: AxisComponent
    :ivar y_temp: Temperature Y axis component.
    :type y_temp: AxisComponent
    :ivar y_prec: Precipitation Y axis component.
    :type y_prec: AxisComponent
    """

    def __init__(self):
        """Initialize the Axis container with empty components."""
        self.x = AxisComponent()
        self.y_temp = AxisComponent()
        self.y_prec = AxisComponent()

    def setup(self, data: dict):
        """Populate child components from configuration.

        :param data: Mapping with keys ``'x'``, ``'y_temp'`` and ``'y_prec'``.
        :type data: dict
        """
        self.x.setup(data['x'])
        self.y_temp.setup(data['y_temp'])
        self.y_prec.setup(data['y_prec'])

    def validate(self, validator: Validator):
        """Validate all axis components.

        :param validator: Validator collecting validation messages.
        :type validator: App.Utils.validator.Validator
        """
        self.x.validate(validator)
        self.y_temp.validate(validator)
        self.y_prec.validate(validator)


class Src:
    """Source configuration holding the file path and axis mapping.

    :ivar path: Resolved source path.
    :type path: pathlib.Path | None
    :ivar axis: Axis mapping information.
    :type axis: Axis
    """

    def __init__(self):
        """Create an empty :class:`Src` holding path and axis mapping."""
        self.path: Optional[Path] = None
        self.axis = Axis()

    def setup(self, data: dict):
        """Populate source configuration from mapping.

        :param data: Mapping containing ``'path'`` and ``'axis'``.
        :type data: dict
        """
        self.path = get_src_path(data['path'])
        self.axis.setup(data['axis'])

    def validate(self, validator: Validator):
        """Ensure the source file exists and validate axis mapping.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        self.axis.validate(validator)


class Dest:
    """Destination configuration for climograph outputs.

    :ivar path: Resolved destination path.
    :type path: pathlib.Path | None
    :ivar filename: Output filename.
    :type filename: str
    :ivar export_png: Whether to export PNG.
    :type export_png: bool
    """

    def __init__(self):
        """Create an empty :class:`Dest` with default values."""
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
        """No-op validate kept for API compatibility.

        :param validator: Validator to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class Lettering:
    """Textual configuration for climographs (titles and axis labels).

    :ivar title: Chart title.
    :type title: str
    :ivar subtitle: Optional subtitle.
    :type subtitle: str | None
    :ivar x_label: X axis label.
    :type x_label: str
    :ivar y_temp_label: Temperature Y axis label.
    :type y_temp_label: str
    :ivar y_prec_label: Precipitation Y axis label.
    :type y_prec_label: str
    """

    def __init__(self):
        """Create default lettering values for climograph plots."""
        self.title = ""
        self.subtitle = None
        self.x_label = ""
        self.y_temp_label = ""
        self.y_prec_label = ""

    def setup(self, data: dict):
        """Populate lettering from mapping.

        :param data: Mapping with keys for title, x_label, y_temp_label and y_prec_label.
        :type data: dict
        """
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)
        self.x_label = data['x_label']
        self.y_temp_label = data['y_temp_label']
        self.y_prec_label = data['y_prec_label']

    def validate(self, validator: Validator):
        """No-op validation for lettering."""
        return


class Figure:
    """Visual options for temperature/precipitation figures.

    :ivar name: Figure name.
    :type name: str
    :ivar color: Hex color string.
    :type color: str
    """

    def __init__(self):
        """Create a :class:`Figure` with default (empty) visual values."""
        self.name = ""
        self.color = ""

    def setup(self, data: dict):
        """Populate figure options from mapping.

        :param data: Mapping with keys ``'name'`` and ``'color'``.
        :type data: dict
        """
        self.name = data['name']
        self.color = data['color']

    def validate(self, validator: Validator):
        """Validate color format.

        :param validator: Validator to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')


class Margin:
    """Simple margins container with defaults.

    :ivar left: Left margin.
    :type left: float
    :ivar right: Right margin.
    :type right: float
    :ivar top: Top margin.
    :type top: float
    :ivar bottom: Bottom margin.
    :type bottom: float
    """

    def __init__(self):
        """Create a :class:`Margin` initialized with default zeros."""
        self.left = 0.0
        self.right = 0.0
        self.top = 0.0
        self.bottom = 0.0

    def setup(self, data: dict):
        """Populate margins from mapping.

        :param data: Optional mapping with margin values.
        :type data: dict
        """
        self.left = data.get('left', 80.0)
        self.right = data.get('right', 80.0)
        self.top = data.get('top', 80.0)
        self.bottom = data.get('bottom', 80.0)

    def validate(self, validator: Validator):
        """No validation performed for margins."""
        return


class Legend:
    """Legend configuration for climograph plots.

    :ivar show_legend: Whether to show the legend.
    :type show_legend: bool
    :ivar y_offset: Vertical offset for the legend.
    :type y_offset: float
    """

    def __init__(self):
        """Initialize legend options with defaults."""
        self.show_legend = True
        self.y_offset = 0.0

    def setup(self, data: dict):
        """Populate legend options from mapping.

        :param data: Mapping that may contain ``'y_offset'``.
        :type data: dict
        """
        self.show_legend = True if data.get('y_offset') else False
        self.y_offset = data.get('y_offset', -0.3)

    def validate(self, validator: Validator):
        """No-op validation for legend."""
        return


class Style:
    """Grouping of lettering, figure settings, margin and legend.

    :ivar lettering: Lettering configuration.
    :type lettering: Lettering
    :ivar figure_temp: Temperature figure options.
    :type figure_temp: Figure
    :ivar figure_prec: Precipitation figure options.
    :type figure_prec: Figure
    :ivar margin: Margin settings.
    :type margin: Margin
    :ivar legend: Legend settings.
    :type legend: Legend
    """

    def __init__(self):
        """Create the style container with default children."""
        self.lettering = Lettering()
        self.figure_temp = Figure()
        self.figure_prec = Figure()
        self.margin = Margin()
        self.legend = Legend()

    def setup(self, data: dict):
        """Populate style from mapping.

        :param data: Mapping with keys ``'lettering'``, ``'figure_temp'``,
                     ``'figure_prec'``, and optional ``'margin'`` and ``'legend'``.
        :type data: dict
        """
        self.lettering.setup(data['lettering'])
        self.figure_temp.setup(data['figure_temp'])
        self.figure_prec.setup(data['figure_prec'])
        self.margin.setup(data.get('margin', {}))
        self.legend.setup(data.get('legend', {}))

    def validate(self, validator: Validator):
        """Validate the style subtree by delegating to children.

        :param validator: Validator to collect validation messages.
        :type validator: App.Utils.validator.Validator
        """
        self.lettering.validate(validator)
        self.figure_temp.validate(validator)
        self.figure_prec.validate(validator)
        self.margin.validate(validator)
        self.legend.validate(validator)


class ClimographModel:
    """Top-level model for climograph plot configuration.

    This class exposes :meth:`setup` and :meth:`validate` methods used by
    controllers to load and validate incoming configuration dictionaries.
    """

    def __init__(self, storage: Storage):
        """Create a new ClimographModel.

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
        """Validate the model and return a :class:`Validator` instance.

        :returns: Validator populated with validation results.
        :rtype: App.Utils.validator.Validator
        """
        validator = Validator(self.storage)

        self.src.validate(validator)

        self.dest.validate(validator)

        self.style.validate(validator)

        return validator
