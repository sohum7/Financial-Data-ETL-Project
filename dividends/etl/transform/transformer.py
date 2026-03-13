# Main transform logic for various data categories to GCS

# Built-in imports
import pandas as pd
import logging

# Shared imports
from shared.clients.gcp.logging import GCPLogger

def transform(df: pd.DataFrame, logger: logging.Logger | GCPLogger) -> pd.DataFrame:
    """
    Transform a nested JSON dataframe using pandas.
    """

    # -----------------------
    # 1. Flatten nested JSON structure
    # -----------------------
    '''
    if 'data' in df.columns:
        df = df.explode('data')
        df = pd.json_normalize(df['data'].tolist())
    else:
        logger.error("Column 'data' not found in DataFrame. Skipping explode step.")
    '''
    # -----------------------
    # 2. Rename columns to match desired schema
    # -----------------------
    df = df.rename(columns={
        "dividend": "dividend_ratio",
        "date": "market_dt",
        "payment_date": "payment_dt",
        "record_date": "record_dt",
        "declaration_date": "declar_dt"
    })

    # -----------------------
    # 3. Drop rows with nulls in essential columns
    # -----------------------
    df = df.dropna(subset=['symbol', 'market_dt', 'dividend_ratio'])

    # -----------------------
    # 4. Fill missing values for non-essential columns
    # -----------------------
    df['distr_freq'] = df.get('distr_freq', pd.Series()).fillna('Unknown')
    #df['payment_dt'] = df.get('payment_dt', pd.Series()).fillna(pd.NaT)
    #df['record_dt'] = df.get('record_dt', pd.Series()).fillna(pd.NaT)
    #df['declar_dt'] = df.get('declar_dt', pd.Series()).fillna(pd.NaT)

    # -----------------------
    # 5. Convert date columns to datetime.date (remove time component)
    # -----------------------
    
    #df['datetime_col'] = pd.to_datetime(df['datetime_col']) # Convert to datetime objects
    
    date_cols = ['market_dt', 'payment_dt', 'record_dt', 'declar_dt']
    for col_name in date_cols:
        if col_name in df.columns:
            df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date

    # -----------------------
    # 6. Reorder columns
    # -----------------------
    columns_order = ['symbol', 'market_dt', 'dividend_ratio', 'distr_freq', \
                    'payment_dt', 'record_dt', 'declar_dt']
    df = df.reindex(columns=[c for c in columns_order if c in df.columns])

    return df