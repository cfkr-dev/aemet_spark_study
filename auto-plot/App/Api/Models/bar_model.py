"""Bar model definitions and configuration containers.

.. module:: App.Api.Models.bar_model

This module provides lightweight data-holder classes used to parse and
validate the configuration for bar plots. Each class implements a
``setup`` method that populates attributes from a configuration mapping
and a ``validate`` method that adds validation messages to a
:class:`App.Utils.validator.Validator` instance.

All docstrings use reST style so they are compatible with Sphinx.
"""

import re

from typing import Optional
from pathlib import Path

from App.Utils.Storage.Core.storage import Storage
from App.Utils.validator import Validator
from App.Utils.file_utils import get_src_path, get_dest_path


class AxisComponent:
    """Represent a single axis component used by plot configurations.

    :ivar name: Human-readable name of the axis component.
    :type name: str
    """

    def __init__(self):
        """Create an empty :class:`AxisComponent`.

        The instance starts with an empty ``name`` and must be populated
        by calling :meth:`setup`.
        """
        self.name = ""

    def setup(self, data: dict):
        """Populate this component from a mapping.

        :param data: Configuration mapping expected to contain the key ``'name'``.
        :type data: dict
        :raises KeyError: If the required key is missing from ``data``.
        """
        self.name = data['name']

    def validate(self, validator: Validator):
        """Validate the axis component.

        This implementation currently performs no validation but keeps the
        method for API compatibility.

        :param validator: Validator object used to collect validation errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class Axis:
    """Container for X/Y axis components used by bar plots.

    :ivar x: Component describing the horizontal axis.
    :type x: AxisComponent
    :ivar y: Component describing the vertical axis.
    :type y: AxisComponent
    """

    def __init__(self):
        """Initialize empty axis components for X and Y."""
        self.x = AxisComponent()
        self.y = AxisComponent()

    def setup(self, data: dict):
        """Populate axis components from a configuration mapping.

        :param data: Mapping with keys ``'x'`` and ``'y'`` containing the
                     configuration for each component.
        :type data: dict
        """
        self.x.setup(data['x'])
        self.y.setup(data['y'])

    def validate(self, validator: Validator):
        """Run validation for both axis components.

        :param validator: Validator to collect validation messages.
        :type validator: App.Utils.validator.Validator
        """
        self.x.validate(validator)
        self.y.validate(validator)


class Src:
    """Source configuration for a bar plot.

    This class holds the path to the input data and the axis mapping.

    :ivar path: Resolved source path returned by :func:`App.Utils.file_utils.get_src_path`.
    :type path: pathlib.Path | None
    :ivar axis: Axis mapping information.
    :type axis: Axis
    """

    def __init__(self):
        """Create an empty :class:`Src` instance."""
        self.path: Optional[Path] = None
        self.axis = Axis()

    def setup(self, data: dict):
        """Populate source configuration from a mapping.

        :param data: Mapping that must include ``'path'`` and ``'axis'`` keys.
        :type data: dict
        """
        self.path = get_src_path(data['path'])
        self.axis.setup(data['axis'])

    def validate(self, validator: Validator):
        """Validate the source configuration.

        The method checks that the resolved source path exists in the
        provided storage and delegates to the axis validator.

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        if not validator.storage.exists(self.path.as_posix()):
            validator.set_invalid()
            validator.add_error_msg('Source path must be a valid path')

        self.axis.validate(validator)


class Dest:
    """Destination configuration for the produced artifact.

    :ivar path: Resolved destination directory.
    :type path: pathlib.Path | None
    :ivar filename: Output filename (without path).
    :type filename: str
    :ivar export_png: Whether to also export a PNG version.
    :type export_png: bool
    """

    def __init__(self):
        """Create an empty destination configuration."""
        self.path: Optional[Path] = None
        self.filename = ""
        self.export_png = True

    def setup(self, data: dict):
        """Populate destination settings from a mapping.

        :param data: Mapping that must contain ``'path'``, ``'filename'`` and
                     ``'export_png'`` keys.
        :type data: dict
        """
        self.path = get_dest_path(data['path'])
        self.filename = data['filename']
        self.export_png = data['export_png']

    def validate(self, validator: Validator):
        """Validate the destination configuration (no-op).

        :param validator: Validator used to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class BuildItem:
    """Item used to build the textual inside information for bars.

    :ivar text_before: Text to display before the value.
    :type text_before: str
    :ivar name: Field name to use from the source row.
    :type name: str
    :ivar text_after: Text to display after the value.
    :type text_after: str
    """

    def __init__(self):
        """Create an empty :class:`BuildItem`."""
        self.text_before = ""
        self.name = ""
        self.text_after = ""

    def setup(self, data: dict):
        """Populate the item from configuration.

        :param data: Mapping with keys ``'text_before'``, ``'name'`` and
                     ``'text_after'``.
        :type data: dict
        """
        self.text_before = data['text_before']
        self.name = data['name']
        self.text_after = data['text_after']

    def validate(self, validator: Validator):
        """Validate the build item (no-op).

        :param validator: Validator to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class InsideInfoItem:
    """Container for inside-info blocks shown within bars.

    :ivar label: Label to display.
    :type label: str
    :ivar build: Sequence of :class:`BuildItem` instances composing the inside info.
    :type build: list[BuildItem]
    """

    def __init__(self):
        """Create an empty :class:`InsideInfoItem`."""
        self.label = ""
        self.build = []

    def setup(self, data: dict):
        """Populate the inside-info item from configuration.

        :param data: Mapping with keys ``'label'`` and ``'build'`` where ``'build'``
                     is an iterable of dictionaries accepted by
                     :class:`BuildItem.setup`.
        :type data: dict
        """
        self.label = data['label']

        for item in data['build']:
            new_item = BuildItem()
            new_item.setup(item)
            self.build.append(new_item)

    def validate(self, validator: Validator):
        """Validate inside-info and ensure it contains at least one item.

        :param validator: Validator to collect issues.
        :type validator: App.Utils.validator.Validator
        """
        if not len(self.build) > 0:
            validator.set_invalid()
            validator.add_error_msg('Build must contain at least one item')

        for item in self.build:
            item.validate(validator)


class Lettering:
    """Textual elements used in the bar figure (titles, labels, inside info).

    :ivar title: Chart title text.
    :type title: str
    :ivar subtitle: Optional subtitle text.
    :type subtitle: str | None
    :ivar x_label: X axis label.
    :type x_label: str
    :ivar y_label: Y axis label.
    :type y_label: str
    :ivar inside_info: List of inside-info items.
    :type inside_info: list[InsideInfoItem]
    """

    def __init__(self):
        """Create default lettering values (mostly empty strings)."""
        self.title = ""
        self.subtitle = None
        self.x_label = ""
        self.y_label = ""
        self.inside_info = []

    def setup(self, data: dict):
        """Populate lettering from configuration.

        :param data: Mapping that must include keys for title, x_label and y_label.
        :type data: dict
        """
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
        """Validate contained inside-info items.

        :param validator: Validator to collect issues.
        :type validator: App.Utils.validator.Validator
        """
        for item in self.inside_info:
            item.validate(validator)


class Figure:
    """Figure-level visual options for bar plots.

    :ivar color: Base color for the bars.
    :type color: str
    :ivar inverted_horizontal_axis: Whether the horizontal axis is inverted.
    :type inverted_horizontal_axis: bool
    :ivar threshold_limit_max_min: Threshold for limiting max/min values.
    :type threshold_limit_max_min: float
    :ivar threshold_perc_limit_outside_text: Threshold for percentage limit outside text.
    :type threshold_perc_limit_outside_text: float
    :ivar range_margin_perc: Margin percentage for range.
    :type range_margin_perc: float
    """

    def __init__(self):
        """Create default figure settings."""
        self.color = ""
        self.inverted_horizontal_axis = False
        self.threshold_limit_max_min = 0.0
        self.threshold_perc_limit_outside_text = 0.0
        self.range_margin_perc = 0.0

    def setup(self, data: dict):
        """Populate figure settings from configuration.

        :param data: Mapping that must contain keys for color, inverted_horizontal_axis,
                     threshold_limit_max_min, threshold_perc_limit_outside_text, and
                     range_margin_perc.
        :type data: dict
        """
        self.color = data['color']
        self.inverted_horizontal_axis = data['inverted_horizontal_axis']
        self.threshold_limit_max_min = data['threshold_limit_max_min']
        self.threshold_perc_limit_outside_text = data['threshold_perc_limit_outside_text']
        self.range_margin_perc = data['range_margin_perc']

    def validate(self, validator: Validator):
        """Validate figure settings.

        :param validator: Validator to collect validation issues.
        :type validator: App.Utils.validator.Validator
        """
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
    """Margin settings for the bar plot layout.

    :ivar left: Left margin size.
    :type left: float
    :ivar right: Right margin size.
    :type right: float
    :ivar top: Top margin size.
    :type top: float
    :ivar bottom: Bottom margin size.
    :type bottom: float
    """

    def __init__(self):
        """Create default margin values."""
        self.left = 0.0
        self.right = 0.0
        self.top = 0.0
        self.bottom = 0.0

    def setup(self, data: dict):
        """Populate margin sizes from configuration.

        :param data: Mapping that may contain keys for left, right, top, and bottom
                     margin sizes. Missing keys will default to 80.0.
        :type data: dict
        """
        self.left = data.get('left', 80.0)
        self.right = data.get('right', 80.0)
        self.top = data.get('top', 80.0)
        self.bottom = data.get('bottom', 80.0)

    def validate(self, validator: Validator):
        """Validate margin settings (no-op).

        :param validator: Validator to collect errors.
        :type validator: App.Utils.validator.Validator
        """
        return


class Style:
    """Complete style configuration for bar plots.

    This class aggregates lettering, figure, and margin settings.

    :ivar lettering: Textual elements configuration.
    :type lettering: Lettering
    :ivar figure: Visual options for the figure.
    :type figure: Figure
    :ivar margin: Margin settings for the layout.
    :type margin: Margin
    """

    def __init__(self):
        """Create a default style configuration."""
        self.lettering = Lettering()
        self.figure = Figure()
        self.margin = Margin()

    def setup(self, data: dict):
        """Populate the style from a configuration mapping.

        :param data: Mapping containing sub-mappings for ``'lettering'``, ``'figure'``,
                      and optional ``'margin'``.
        :type data: dict
        """
        self.lettering.setup(data['lettering'])
        self.figure.setup(data['figure'])
        self.margin.setup(data.get('margin', {}))

    def validate(self, validator: Validator):
        """Validate the complete style configuration.

        :param validator: Validator to collect any configuration issues.
        :type validator: App.Utils.validator.Validator
        """
        self.lettering.validate(validator)
        self.figure.validate(validator)
        self.margin.validate(validator)


class BarModel:
    """Complete configuration for a bar plot.

    This class aggregates source, destination, and style settings.

    :ivar storage: Storage backend used for file operations.
    :type storage: App.Utils.Storage.Core.storage.Storage
    :ivar src: Source data and axis configuration.
    :type src: Src
    :ivar dest: Destination configuration for the output.
    :type dest: Dest
    :ivar style: Style configuration for the plot.
    :type style: Style
    """

    def __init__(self, storage: Storage):
        """Create a new bar model configuration.

        :param storage: Storage backend instance.
        :type storage: App.Utils.Storage.Core.storage.Storage
        """
        self.storage = storage
        self.src = Src()
        self.dest = Dest()
        self.style = Style()

    def setup(self, data: dict):
        """Populate the bar model from a configuration mapping.

        :param data: Mapping containing sub-mappings for ``'src'``, ``'dest'``, and
                     ``'style'``.
        :type data: dict
        """
        self.src.setup(data['src'])
        self.dest.setup(data['dest'])
        self.style.setup(data['style'])

    def validate(self):
        """Validate the entire bar model configuration.

        This method creates a validator instance and delegates validation
        of each sub-component.

        :returns: Validator instance containing any validation errors.
        :rtype: App.Utils.validator.Validator
        """
        validator = Validator(self.storage)

        self.src.validate(validator)

        self.dest.validate(validator)

        self.style.validate(validator)

        return validator
