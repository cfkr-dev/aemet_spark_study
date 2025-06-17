from enum import Enum
from typing import Type, Optional
from Config.constants import (K_FORMATTER_TIMESTAMP, K_FORMATTER_TIMESTAMP_YEAR)


def get_enum_value(value: str, enum: Type[Enum]) -> Optional[Enum]:
    try:
        return enum(value)
    except ValueError:
        return None


class Formatters(Enum):
    TIMESTAMP = K_FORMATTER_TIMESTAMP
    TIMESTAMP_YEAR = K_FORMATTER_TIMESTAMP_YEAR

