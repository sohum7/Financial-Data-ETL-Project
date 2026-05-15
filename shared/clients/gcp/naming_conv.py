# GCS naming conventions for files and paths used in the project

# Built-in imports
from dataclasses import dataclass, field
from typing import ClassVar

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
    _gcs_prefix: ClassVar[str] = field(init=False)
    
    def __post_init__(self):
        self.bucket = self.bucket.strip('/')
        self.dir    = self.dir.strip('/')
        self.name   = self.name.lstrip('/')
        self.type   = self.type.lower()
        GCSPathLib._gcs_prefix = "gs://"
    
    def file_nm(self, include_type=True):
        return f"{self.name}{f'.{self.type}' if include_type else ''}"
    def blob_nm(self):
        return f"{self.dir}/{self.file_nm(include_type=True)}"
    def blob_path(self, include_prefix=True):
        bp = f"{self.bucket}/{self.blob_nm()}"
        return GCSPathLib._gcs_prefix + bp if include_prefix else bp
    @staticmethod
    def blob_path_static(bucket_nm, blob_nm, include_prefix=True):
        bp = f"{bucket_nm.strip('/')}/{blob_nm.strip('/')}"
        return GCSPathLib._gcs_prefix + bp if include_prefix else bp
    def bucket_blob_nms(self):
        return self.bucket, self.blob_nm()
    def getVars(self):
        return self.blob_path(include_prefix=True), self.blob_nm(), self.bucket, self.dir, self.file_nm(include_type=True)
