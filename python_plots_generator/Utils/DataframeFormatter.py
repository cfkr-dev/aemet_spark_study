import pandas as pd

from Config.enumerations import Formatters

def format_df(df: pd.DataFrame, name_to_format: dict) -> pd.DataFrame:
    format_functions = {
        Formatters.TIMESTAMP: lambda col: pd.to_datetime(col),
        Formatters.TIMESTAMP_YEAR: lambda col: pd.to_datetime(col, format='%Y'),
    }

    for name, format in name_to_format.items():
        if name in df.columns and format in format_functions:
            df[name] = format_functions[format](df[name])

    return df