import pandas as pd

FORMATTERS = {
    'timestamp': lambda col: pd.to_datetime(col),
    'timestamp_year': lambda col: pd.to_datetime(col, format='%Y'),
}

def format_df(df: pd.DataFrame, format_schema: dict) -> pd.DataFrame:
    for columna, tipo in format_schema.items():
        if columna in df.columns and tipo in FORMATTERS:
            df[columna] = FORMATTERS[tipo](df[columna])

    return df