"""Enumerations and helper utilities used across the project.

.. module:: App.Config.enumerations

This module defines application-specific Enum classes and a small helper
function used to resolve string values into Enum members safely.

Enumerations provided:
- :class:`Formatters`: formatter identifiers used by plotters.
- :class:`TableAligns`: alignment options for table rendering.
- :class:`MonthsSp`: abbreviated month names in English used in charts.
- :class:`SpainGeographicLocations`: region identifiers used by geographic plots.
"""

from enum import Enum
from typing import Type, Optional

from App.Config.constants import (
    K_FORMATTER_TIMESTAMP, K_FORMATTER_TIMESTAMP_YEAR,
    K_TABLE_ALIGNS_LEFT, K_TABLE_ALIGNS_CENTER, K_TABLE_ALIGNS_RIGHT,
    K_MONTHS_EN_ENE, K_MONTHS_EN_FEB, K_MONTHS_EN_MAR,
    K_MONTHS_EN_ABR, K_MONTHS_EN_MAY, K_MONTHS_EN_JUN,
    K_MONTHS_EN_JUL, K_MONTHS_EN_AGO, K_MONTHS_EN_SEP,
    K_MONTHS_EN_OCT, K_MONTHS_EN_NOV, K_MONTHS_EN_DIC,
    K_SPAIN_GEOGRAPHIC_LOCATIONS_CONTINENTAL, K_SPAIN_GEOGRAPHIC_LOCATIONS_CANARY_ISLANDS,
)


def get_enum_value(value: str, enum: Type[Enum]) -> Optional[Enum]:
    """Return the Enum member corresponding to ``value`` or ``None``.

    This helper wraps the direct Enum lookup to avoid raising a
    ValueError when the provided string does not match any member. It is
    used across the project when parsing configuration dictionaries.

    :param value: The string value to convert to an Enum member.
    :type value: str
    :param enum: The Enum class to resolve the value against.
    :type enum: typing.Type[enum.Enum]
    :returns: The matching Enum member or ``None`` if not found.
    :rtype: Optional[Enum]
    """
    try:
        return enum(value)
    except ValueError:
        return None


class Formatters(Enum):
    """Available formatter identifiers used by plotters.

    Members map to the string constants defined in
    :mod:`App.Config.constants`.

    :cvar TIMESTAMP: Formatter that formats full timestamps.
    :cvar TIMESTAMP_YEAR: Formatter that extracts the year from timestamps.
    """

    TIMESTAMP = K_FORMATTER_TIMESTAMP
    TIMESTAMP_YEAR = K_FORMATTER_TIMESTAMP_YEAR


class TableAligns(Enum):
    """Column alignment options for tabular renderers.

    Values correspond to simple textual alignment keywords used by the
    table plotter.

    :cvar LEFT: Left alignment.
    :cvar CENTER: Center alignment.
    :cvar RIGHT: Right alignment.
    """

    LEFT = K_TABLE_ALIGNS_LEFT
    CENTER = K_TABLE_ALIGNS_CENTER
    RIGHT = K_TABLE_ALIGNS_RIGHT


class MonthsSp(Enum):
    """Abbreviated month names in English used for display in plots.

    Members use the short three-letter English month abbreviations.
    """

    JAN = K_MONTHS_EN_ENE
    FEB = K_MONTHS_EN_FEB
    MAR = K_MONTHS_EN_MAR
    APR = K_MONTHS_EN_ABR
    MAY = K_MONTHS_EN_MAY
    JUN = K_MONTHS_EN_JUN
    JUL = K_MONTHS_EN_JUL
    AUG = K_MONTHS_EN_AGO
    SEP = K_MONTHS_EN_SEP
    OCT = K_MONTHS_EN_OCT
    NOV = K_MONTHS_EN_NOV
    DIC = K_MONTHS_EN_DIC

class SpainGeographicLocations(Enum):
    """Geographic location identifiers for Spain used by geographic plots.

    :cvar CONTINENTAL: Continental Spain.
    :cvar CANARY_ISLANDS: The Canary Islands.
    """

    CONTINENTAL = K_SPAIN_GEOGRAPHIC_LOCATIONS_CONTINENTAL
    CANARY_ISLANDS = K_SPAIN_GEOGRAPHIC_LOCATIONS_CANARY_ISLANDS
