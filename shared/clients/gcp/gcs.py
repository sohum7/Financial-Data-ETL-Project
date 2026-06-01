# GCS naming conventions for files and paths

# Built-in imports
from dataclasses import dataclass
from typing import ClassVar


# GCS file naming convention
MS_FILE_NM = lambda data_type, START_DT, END_DT, HASH_VAL: \
    f"{data_type.lower()}_{START_DT}_{END_DT}_{HASH_VAL}"

# GCS path metadata
@dataclass(slots=True, frozen=True)
class FileConfig:
    """GCS file path data"""
    gcs_prefix: ClassVar[str] = "gs://"
    
    bucket_nm: str
    dir_path: str
    file_nm: str
    file_type: str
    
    def __post_init__(self):
        # Instance variable type checking
        self.type_check_vars()
        
        # Instance variable cleaning
        self.clean_vars()
    
    # static method to create a full blob path (does include "gs://{bucket_nm}")
    @staticmethod
    def blob_path_static(bucket_nm, blob_nm, include_prefix=True):
        bp = f"{bucket_nm.strip('/')}/{blob_nm.strip('/')}"
        return FileConfig.gcs_prefix + bp if include_prefix else bp
    
    # Type check instance variables
    def type_check_vars(self):
        fields = [
            ("bucket_nm", self.bucket_nm, str),
            ("dir_path",  self.dir_path,  str),
            ("file_nm",   self.file_nm,   str),
            ("file_type", self.file_type, str),
        ]
        
        for name, value, typ in fields:
            if not isinstance(value, str):
                raise TypeError(f"{name} must be {typ}")
    
    # Clean instance variables
    def clean_vars(self):
        fields = [
            ("bucket_nm", self.bucket_nm.strip('/')),
            ("dir_path",  self.dir_path.strip('/')),
            ("file_nm",   self.file_nm.strip('/')),
            ("file_type", self.file_type.lower().strip().lstrip(".")),
        ]
        
        for name, new_val in fields:
            object.__setattr__(self, name, new_val)
    
    # Get the blob name (does not include "gs://{bucket_nm}")
    @property
    def blob_nm(self):
        return f"{self.dir_path}/{self.file_nm}.{self.file_type}"
    
    # Create a full blob path (does include "gs://{bucket_nm}")
    @property
    def blob_path(self, include_prefix=True):
        return FileConfig.blob_path_static(self.bucket_nm, self.blob_nm, include_prefix)
    
    def __str__(self):
        return f"bucket name: {self.bucket_nm}\ndirectory path: {self.dir_path}\nfile name: {self.file_nm}\nfile type: {self.file_type}"
    
    def __repr__(self):
        return f"FileConfig(bucket_nm={self.bucket_nm}, dir_path={self.dir_path}, file_nm={self.file_nm}, file_type={self.file_type})"
