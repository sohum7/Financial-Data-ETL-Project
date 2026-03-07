# Main entry point for the transform step of the dividends data category

# Shared imports
from shared.configs.config_loader import MS_CAT, MS_DIV_RAW_FILE_BUCKET_NM, MS_DIV_RAW_FILE_BUCKET_SUBDIR, MS_DIV_TFD_FILE_BUCKET_NM, MS_DIV_TFD_FILE_BUCKET_SUBDIR, MS_DIV_TFD_FILE_TYPE
from transform.src.dataproc_job import transform, transform_dividends

def main():
    # Required parameters
    request = {
        "data_cat": MS_CAT,
        "raw_bucket_nm": MS_DIV_RAW_FILE_BUCKET_NM,
        "raw_dir_nm": MS_DIV_RAW_FILE_BUCKET_SUBDIR,
        "tfd_bucket_nm": MS_DIV_TFD_FILE_BUCKET_NM,
        "tfd_dir_nm": MS_DIV_TFD_FILE_BUCKET_SUBDIR,
        "tfd_file_type": MS_DIV_TFD_FILE_TYPE,
        "tfd_save_mode": "append",
        "batch_dt": "2026-03-07",
        "start_dt": "2026-03-02",
        "end_dt": "2026-03-06"
    }

    transform_dividends(request)

if __name__ == "__main__":
    main()