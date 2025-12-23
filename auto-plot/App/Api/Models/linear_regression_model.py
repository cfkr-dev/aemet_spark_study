"""Linear regression model containers and validation helpers.

.. module:: App.Api.Models.linear_regression_model

Defines containers used to parse configurations for linear regression
plots. Each container exposes :meth:`setup` and :meth:`validate`.
"""

import re

from pathlib import Path

from App.Config.enumerations import *
from App.Utils.Storage.Core.storage import Storage
from App.Utils.file_utils import get_src_path, get_dest_path
from App.Utils.validator import Validator


class AxisComponent:
    """Axis component with an optional formatter enum.

    :ivar name: Component name.
    :type name: str
    :ivar format: Resolved formatter enum or None.
    :type format: object | None
    """

    def __init__(self):
        """Create an empty :class:`AxisComponent` with default values."""
        self.name = ""
        self.format = None

    def setup(self, data: dict):
        """Populate component from mapping.

        :param data: Mapping that may contain ``'name'`` and ``'format'``.
        :type data: dict
        """
        self.name = data['name']
        self.format = get_enum_value(data.get('format'), Formatters)

    def validate(self, validator: Validator):
        """No-op validation for axis component."""
        return


class Axis:
    """Container for X/Y axis components."""

    def __init__(self):
        """Initialize axis components."""
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
        """Validate both axis components.

        :param validator: Validator used to collect messages.
        :type validator: App.Utils.validator.Validator
        """
        self.x.validate(validator)
        self.y.validate(validator)


class Main:
    """Main data source specification for regression model.

    :ivar path: Resolved path to the main dataset.
    :type path: pathlib.Path | None
    :ivar axis: Axis mapping for the main dataset.
    :type axis: Axis
    """

    def __init__(self):
        """Create an empty :class:`Main` with default path and axis."""
        self.path: Optional[Path] = None
        self.axis = Axis()

    def setup(self, data: dict):
        """Populate main source settings from mapping.

        :param data: Mapping containing ``'path'`` and ``'axis'``.
        :type data: dict
        """
        self.path = get_src_path(data['path'])
        self.axis.setup(data['axis'])

    def validate(self, validator: Validator):
        """Validate the main source path exists and validate its axis mapping.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Main source path must be a valid path')

        self.axis.validate(validator)


class Names:
    """Names container for slope and intercept columns."""

    def __init__(self):
        """Initialize names to empty strings."""
        self.slope = ""
        self.intercept = ""

    def setup(self, data: dict):
        """Populate names from mapping.

        :param data: Mapping with keys ``'slope'`` and ``'intercept'``.
        :type data: dict
        """
        self.slope = data['slope']
        self.intercept = data['intercept']

    def validate(self, validator: Validator):
        """No-op validation for names."""
        return


class Regression:
    """Regression source configuration (path and column names)."""

    def __init__(self):
        """Create an empty :class:`Regression` with default values."""
        self.path: Optional[Path] = None
        self.names = Names()

    def setup(self, data: dict):
        """Populate regression source settings from mapping.

        :param data: Mapping containing ``'path'`` and ``'names'``.
        :type data: dict
        """
        self.path = get_src_path(data['path'])
        self.names.setup(data['names'])

    def validate(self, validator: Validator):
        """Validate regression source exists and its names mapping."""
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Regression source path must be a valid path')

        self.names.validate(validator)


class Src:
    """Grouping of main and regression sources."""

    def __init__(self):
        """Create a :class:`Src` grouping main and regression sources."""
        self.main = Main()
        self.regression = Regression()

    def setup(self, data: dict):
        """Populate grouped sources from mapping.

        :param data: Mapping with keys ``'main'`` and ``'regression'``.
        :type data: dict
        """
        self.main.setup(data['main'])
        self.regression.setup(data['regression'])

    def validate(self, validator: Validator):
        """Validate both main and regression sources."""
        self.main.validate(validator)
        self.regression.validate(validator)


class Dest:
    """Destination configuration for regression outputs."""

    def __init__(self):
        """Create an empty :class:`Dest` for regression outputs."""
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
    """Textual labels for regression plots (title and axis labels)."""

    def __init__(self):
        """Create default lettering values for regression plots."""
        self.title = ""
        self.subtitle = None
        self.x_label = ""
        self.y_1_label = ""
        self.y_2_label = ""

    def setup(self, data: dict):
        """Populate lettering from mapping.

        :param data: Mapping containing title and axis label keys.
        :type data: dict
        """
        self.title = data['title']
        self.subtitle = data.get('subtitle', None)
        self.x_label = data['x_label']
        self.y_1_label = data['y_1_label']
        self.y_2_label = data['y_2_label']

    def validate(self, validator: Validator):
        """No-op validation for lettering."""
        return


class Figure:
    """Figure appearance descriptor for regression plots."""

    def __init__(self):
        """Create an empty :class:`Figure` for regression visuals."""
        self.name = ""
        self.color = ""

    def setup(self, data: dict):
        """Populate figure settings from mapping.

        :param data: Mapping with keys ``'name'`` and ``'color'``.
        :type data: dict
        """
        self.name = data['name']
        self.color = data['color']

    def validate(self, validator: Validator):
        """Validate color format for the figure."""
        if not bool(re.fullmatch(r'#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})', self.color)):
            validator.set_invalid()
            validator.add_error_msg('Color must be a valid hex color')


class Margin:
    """Margins container with default fallbacks."""

    def __init__(self):
        """Initialize margin defaults."""
        self.left = 0.0
        self.right = 0.0
        self.top = 0.0
        self.bottom = 0.0

    def setup(self, data: dict):
        """Populate margin values from mapping."""
        self.left = data.get('left', 80.0)
        self.right = data.get('right', 80.0)
        self.top = data.get('top', 80.0)
        self.bottom = data.get('bottom', 80.0)

    def validate(self, validator: Validator):
        """No-op validation for margins."""
        return


class Legend:
    """Legend options wrapper.

    :ivar show_legend: Whether the legend is shown.
    :type show_legend: bool
    :ivar y_offset: Vertical offset for the legend.
    :type y_offset: float
    """

    def __init__(self):
        """Initialize legend defaults for regression plots."""
        self.show_legend = True
        self.y_offset = 0.0

    def setup(self, data: dict):
        """Populate legend settings from mapping.

        :param data: Mapping that may contain the key ``'y_offset'``.
        :type data: dict
        """
        self.show_legend = True if data.get('y_offset') else False
        self.y_offset = data.get('y_offset', -0.3)

    def validate(self, validator: Validator):
        """No-op validation for legend; kept for interface consistency.

        :param validator: Validator used to collect issues.
        :type validator: App.Utils.validator.Validator
        """
        return


class Style:
    """Style grouping for regression model outputs.

    :ivar lettering: Lettering settings.
    :type lettering: Lettering
    :ivar figure_1: First figure settings.
    :type figure_1: Figure
    :ivar figure_2: Second figure settings.
    :type figure_2: Figure
    :ivar margin: Margin settings.
    :type margin: Margin
    :ivar legend: Legend settings.
    :type legend: Legend
    """

    def __init__(self):
        """Create the style container with its child components."""
        self.lettering = Lettering()
        self.figure_1 = Figure()
        self.figure_2 = Figure()
        self.margin = Margin()
        self.legend = Legend()

    def setup(self, data: dict):
        """Populate the style configuration from mapping.

        :param data: Mapping with keys ``'lettering'``, ``'figure_1'``, ``'figure_2'`` and optional
                     ``'margin'`` and ``'legend'``.
        :type data: dict
        """
        self.lettering.setup(data['lettering'])
        self.figure_1.setup(data['figure_1'])
        self.figure_2.setup(data['figure_2'])
        self.margin.setup(data.get('margin', {}))
        self.legend.setup(data.get('legend', {}))

    def validate(self, validator: Validator):
        """Validate the style subtree by delegating to child validators.

        :param validator: Validator used to collect validation issues.
        :type validator: App.Utils.validator.Validator
        """
        self.lettering.validate(validator)
        self.figure_1.validate(validator)
        self.figure_2.validate(validator)
        self.margin.validate(validator)
        self.legend.validate(validator)


class LinearRegressionModel:
    """Top-level model for linear regression configuration."""

    def __init__(self, storage: Storage):
        """Create a new LinearRegressionModel.

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
