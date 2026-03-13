# 

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.clients.ms.ms_api import ms_api_request


def extract(ms_data_cat_url: str, ms_symbols_lst_str: str, ms_api_key: str, batch_dt: str, start_dt: str, end_dt: str, logger, **kwargs):
    return ms_api_request(ms_data_cat_url, ms_symbols_lst_str, ms_api_key, batch_dt, start_dt, end_dt, logger, **kwargs)