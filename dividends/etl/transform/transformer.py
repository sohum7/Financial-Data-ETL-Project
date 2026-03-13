# Main transform logic for various data categories to GCS

# Builtin imports
import pandas as pd
import logging

def transform(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Transform a nested JSON dataframe using pandas.
    """

    # -----------------------
    # 1. Flatten nested JSON structure
    # -----------------------
    if 'data' in df.columns:
        df = df.explode('data')
        df = pd.json_normalize(df['data'].tolist())
    else:
        logger.error("Column 'data' not found in DataFrame. Skipping explode step.")

    # -----------------------
    # 2. Rename columns to match desired schema
    # -----------------------
    df = df.rename(columns={
        "dividend": "dividend_ratio",
        "date": "market_dt",
        "payment_date": "pay_dt",
        "record_date": "record_dt",
        "declaration_date": "decl_dt"
    })

    # -----------------------
    # 3. Drop rows with nulls in essential columns
    # -----------------------
    df = df.dropna(subset=['symbol', 'market_dt', 'dividend_ratio'])

    # -----------------------
    # 4. Fill missing values for non-essential columns
    # -----------------------
    df['distr_freq'] = df.get('distr_freq', pd.Series()).fillna('Unknown')
    df['payment_dt'] = df.get('payment_dt', pd.Series()).fillna(pd.NaT)
    df['record_dt'] = df.get('record_dt', pd.Series()).fillna(pd.NaT)
    df['declar_dt'] = df.get('declar_dt', pd.Series()).fillna(pd.NaT)

    # -----------------------
    # 5. Convert date columns to datetime.date (remove time component)
    # -----------------------
    date_cols = ['market_dt', 'payment_dt', 'record_dt', 'declar_dt']
    for col_name in date_cols:
        if col_name in df.columns:
            df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                df[col_name] = df[col_name].dt.date
            else:
                df[col_name] = df[col_name].apply(lambda x: x.date() if pd.notnull(x) and hasattr(x, 'date') else pd.NaT)

    # -----------------------
    # 6. Reorder columns
    # -----------------------
    columns_order = ['symbol', 'market_dt', 'dividend_ratio', 'distr_freq', \
                    'payment_dt', 'record_dt', 'declar_dt']
    df = df.reindex(columns=[c for c in columns_order if c in df.columns])

    return df