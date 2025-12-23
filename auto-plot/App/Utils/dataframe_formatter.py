"""Small helpers to format pandas DataFrames according to configuration.

This module exposes :func:`format_df` which applies simple column-wise
formatters to a :class:`pandas.DataFrame` using the
:class:`App.Config.enumerations.Formatters` enumeration.
"""

import pandas as pd

from App.Config.enumerations import Formatters


def format_df(df: pd.DataFrame, name_to_format: dict) -> pd.DataFrame:
    """Format columns in ``df`` according to ``name_to_format`` mapping.

    The ``name_to_format`` mapping must map column names to members of
    :class:`Formatters`. Only columns present in the dataframe and
    present in the mapping will be transformed.

    :param df: DataFrame to format in-place (a new DataFrame reference is returned).
    :type df: pandas.DataFrame
    :param name_to_format: Mapping of column name -> :class:`Formatters` member.
    :type name_to_format: dict[str, App.Config.enumerations.Formatters]
    :returns: The same :class:`pandas.DataFrame` instance with formatted columns.
    :rtype: pandas.DataFrame
    """
    format_functions = {
        Formatters.TIMESTAMP: lambda col: pd.to_datetime(col),
        Formatters.TIMESTAMP_YEAR: lambda col: pd.to_datetime(col, format='%Y'),
    }

    for name, format in name_to_format.items():
        if name in df.columns and format in format_functions:
            df[name] = format_functions[format](df[name])

    return df