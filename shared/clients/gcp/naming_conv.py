# GCS naming conventions for files and paths used in the project
# Builtin imports
from dataclasses import dataclass

# GCS file naming convention
MS_FILE_NM = lambda DATA_CAT, START_DT, END_DT, HASH_VAL: \
    f"{DATA_CAT.lower()}_{START_DT}_{END_DT}_{HASH_VAL}"

# GCS file path and blob naming convention
GCS_PREFIX = "gs://"

GCS_BUCKET_PATH = lambda BUCKET_NM: \
    f"{BUCKET_NM.lstrip('/')}{'' if BUCKET_NM.endswith('/') else '/'}"

GCS_DIR_PATH = lambda DIR: \
    f"{DIR.lstrip('/')}{'' if DIR.endswith('/') else '/'}"
    
GCS_FILE_NM_W_EXT = lambda FILE_NM, FILE_TYPE: \
    f"{FILE_NM.lstrip('/')}.{FILE_TYPE.lower()}"

GCS_BLOB_NM = lambda DIR, FILE_NM, FILE_TYPE: \
    f"{GCS_DIR_PATH(DIR)}{GCS_FILE_NM_W_EXT(FILE_NM, FILE_TYPE)}"
    
GCS_BLOB_PATH_V1 = lambda BUCKET_NM, BLOB_NM: \
    f"{GCS_PREFIX}{GCS_BUCKET_PATH(BUCKET_NM)}{BLOB_NM}"

GCS_BLOB_PATH_V2 = lambda BUCKET_NM, DIR, FILE_NM, FILE_TYPE: \
    f"{GCS_PREFIX}{GCS_BUCKET_PATH(BUCKET_NM)}{GCS_BLOB_NM(DIR, FILE_NM, FILE_TYPE)}"

#DF_SAVE_PATH = lambda BUCKET_NM, DIR: \
#    f"{GCS_BUCKET_PATH(BUCKET_NM)}{GCS_DIR_PATH(DIR)}"

@dataclass(slots=True)
class GCSPathLib:
    bucket: str
    dir: str
    name: str
    type: str
    
    def __post_init__(self):
        self.bucket = self.bucket.strip('/')
        self.dir    = self.dir.strip('/')
        self.name   = self.name.lstrip('/')
        self.type   = self.type.lower()
    
    def file_nm(self, include_type=True):
        return f"{self.name}{f'.{self.type}' if include_type else ''}"
    def file_type(self):
        return self.type
    def blob_nm(self):
        return f"{self.dir}/{self.file_nm(include_type=True)}"
    def blob_path(self, include_prefix=True):
        return f"{'gs://' if include_prefix else ''}{self.bucket}/{self.blob_nm()}"
    @staticmethod
    def blob_path_static(bucket, dir):
        f"{bucket.strip('/')}/{dir}"
    def getVars(self):
        return self.blob_path(include_prefix=True), self.blob_nm(), self.bucket, self.dir, self.file_nm(include_type=True)