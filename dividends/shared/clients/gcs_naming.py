# GCS naming conventions for files and paths used in the project

# GCS file naming convention
MS_FILE_NM = lambda DATA_CAT, START_DT, END_DT: \
    f"{DATA_CAT.lower()}_{START_DT}_{END_DT}"
MS_FILE_NM_W_EXT = lambda DATA_CAT, START_DT, END_DT, FILE_TYPE: \
    f"{MS_FILE_NM(DATA_CAT, START_DT, END_DT)}.{FILE_TYPE.lower()}"

# GCS file path and blob naming convention
GCS_PREFIX = "gs://"
GCS_BUCKET_PATH = lambda BUCKET_NM: \
    f"{GCS_PREFIX}{BUCKET_NM}"
GCS_BLOB_PATH_PREFIX = lambda BATCH_DT, DIR_NM: \
    f"{DIR_NM}/batch_date={BATCH_DT}"
GCS_BLOB_PATH = lambda BATCH_DT, DIR_NM, FILE_NM: \
    f"{GCS_BLOB_PATH_PREFIX(BATCH_DT, DIR_NM)}/{MS_FILE_NM_W_EXT(FILE_NM, '', '', 'json')}"