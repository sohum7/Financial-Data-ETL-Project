DIVIDENDS_CONFIG = {
    "columns": [
        ("symbol", "STRING", "NOT NULL"),
        ("market_dt", "DATE", "NOT NULL"),
        ("dividend_ratio", "FLOAT64", "NOT NULL"),
        ("distr_freq", "STRING"),
        ("payment_dt", "DATE"),
        ("record_dt", "DATE"),
        ("declar_dt", "DATE"),
    ],
    "primary_cols": ["symbol", "market_dt"]
}

def get_columns(data_type: str) -> list[tuple[str, ...]]:
    """Retrieve the columns w/ constraints for the given data type"""
    
    if data_type == "dividends": 
        return DIVIDENDS_CONFIG["columns"]
    else:
        raise ValueError(f"ERROR: Unsupported Data Type: {data_type}")
    
def get_primary_columns(data_type: str) -> list[str]:
    """Retrieve primary columns for the given data type"""
    
    if data_type == "dividends": 
        return DIVIDENDS_CONFIG["primary_cols"]
    else:
        raise ValueError(f"ERROR: Unsupported Data Type: {data_type}")