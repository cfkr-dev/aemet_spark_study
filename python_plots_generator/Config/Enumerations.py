from enum import Enum
from typing import Type, Optional
from Config.Constants import (
    K_FORMATTER_TIMESTAMP, K_FORMATTER_TIMESTAMP_YEAR,
    K_TABLE_ALIGNS_LEFT, K_TABLE_ALIGNS_CENTER, K_TABLE_ALIGNS_RIGHT,
    K_MONTHS_SP_ENE, K_MONTHS_SP_FEB, K_MONTHS_SP_MAR,
    K_MONTHS_SP_ABR, K_MONTHS_SP_MAY, K_MONTHS_SP_JUN,
    K_MONTHS_SP_JUL, K_MONTHS_SP_AGO, K_MONTHS_SP_SEP,
    K_MONTHS_SP_OCT, K_MONTHS_SP_NOV, K_MONTHS_SP_DIC
)


def get_enum_value(value: str, enum: Type[Enum]) -> Optional[Enum]:
    try:
        return enum(value)
    except ValueError:
        return None


class Formatters(Enum):
    TIMESTAMP = K_FORMATTER_TIMESTAMP
    TIMESTAMP_YEAR = K_FORMATTER_TIMESTAMP_YEAR


class TableAligns(Enum):
    LEFT = K_TABLE_ALIGNS_LEFT
    CENTER = K_TABLE_ALIGNS_CENTER
    RIGHT = K_TABLE_ALIGNS_RIGHT


class MonthsSp(Enum):
    ENE = K_MONTHS_SP_ENE
    FEB = K_MONTHS_SP_FEB
    MAR = K_MONTHS_SP_MAR
    ABR = K_MONTHS_SP_ABR
    MAY = K_MONTHS_SP_MAY
    JUN = K_MONTHS_SP_JUN
    JUL = K_MONTHS_SP_JUL
    AGO = K_MONTHS_SP_AGO
    SEP = K_MONTHS_SP_SEP
    OCT = K_MONTHS_SP_OCT
    NOV = K_MONTHS_SP_NOV
    DIC = K_MONTHS_SP_DIC
