# GCS naming conventions for files and paths used in the project

# GCS file naming convention
MS_FILE_NM = lambda DATA_CAT, START_DT, END_DT, HASH_VAL: \
    f"{DATA_CAT.lower()}_{START_DT}_{END_DT}_{HASH_VAL}"

# GCS file path and blob naming convention
GCS_PREFIX = "gs://"

GCS_BUCKET_PATH = lambda BUCKET_NM: \
    f"{GCS_PREFIX}{BUCKET_NM}{'/' if BUCKET_NM.endswith('/') else '/'}"

GCS_DIR_PATH = lambda DIR: \
    f"{DIR}{'' if DIR.endswith('/') else '/'}"

GCS_BLOB_PATH = lambda DIR, FILE_NM, FILE_TYPE: \
    f"{GCS_DIR_PATH(DIR)}{FILE_NM}.{FILE_TYPE.lower()}"

DF_SAVE_PATH = lambda BUCKET_NM, DIR: \
    f"{GCS_BUCKET_PATH(BUCKET_NM)}{GCS_DIR_PATH(DIR)}"
