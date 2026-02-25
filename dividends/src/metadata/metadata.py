# Metadata holder for ETL process. This can be used to store any metadata related to the ETL process, such as data source information, transformation logic, etc.
## ms_metadata table in bigquery can be used to store metadata related to the ETL process. This table can have columns such as data_category, source_url, api_key_used, transformation_logic, load_target, etc. This will help in tracking the ETL process and also in debugging any issues that may arise during the process. We can also have a separate table for each data category if needed, but for now we will have a single table to store metadata for all data categories.
# column names
## dat_cat (dividends, ticketers, etc.)
## start_dt (start date of the data extraction)
## end_dt (end date of the data extraction)
## batch_dt (date of the batch run)
## extract_status (success, failure, running, retrying, etc.)
## transform_status (success, failure, running, retrying, etc.)
## load_status (success, failure, running, retrying, etc.)
## start_time (timestamp when the batch started)
## end_time (timestamp when the batch ended)