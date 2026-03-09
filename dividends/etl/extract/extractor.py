# 

# Shared imports
from shared.clients.ms_api import ms_api_request


def extract(data_cat, symbols_lst_str: str, batch_dt: str, start_dt: str, end_dt: str, logger, **kwargs):
    return ms_api_request(data_cat, symbols_lst_str, batch_dt, start_dt, end_dt, logger, **kwargs)