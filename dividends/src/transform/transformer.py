from pyspark.sql.functions import col, explode
from dividends.src.clients.gcp_services import read_json_from_gcs, write_dividends_df_to_gcs

def transform_dividends(data_cat, raw_bucket_nm, raw_dir_nm, tfd_bucket_nm, tfd_dir_nm, batch_dt, start_dt, end_dt, **kwargs):
        tfd_file_type = kwargs.get("tfd_file_type", "delta")
        tfd_save_mode = kwargs.get("save_mode", "append")
        
        try:
                df = read_json_from_gcs(data_cat, raw_bucket_nm, raw_dir_nm, batch_dt, with_spark=True, **kwargs) # read_json_from_gcs_with_spark(spark, raw_bucket_nm, raw_dir_nm, batch_dt)
        except Exception as e:
                return {"status": "error", "message": f"Error reading raw JSON data from GCS: {e}"}, 500
        
        # Flatten the nested JSON structure and select the relevant fields
        df = df.select(explode(col("data")).alias("record")).select("record.*")
        
        # Rename columns to match the desired schema
        df = df \
                .withColumnRenamed("dividend", "dividend_ratio") \
                .withColumnRenamed("date",  "market_dt") \
                .withColumnRenamed("payment_date", "payment_dt") \
                .withColumnRenamed("record_date",  "record_dt") \
                .withColumnRenamed("declaration_date",  "declar_dt") \
                .withColumnRenamed("record_date",  "record_dt")
        
        # Replace null values in essential columns with default values or drop rows with null values based on the use case. For now, we will drop rows with null values in symbol, market_dt, and dividend_ratio columns since they are essential for analysis and downstream processing. We will replace null values in other columns with default values.
        df = df.dropna(subset=['symbol', 'market_dt', 'dividend_ratio']) # drop rows with null values in symbol or market_dt columns since they are essential for analysis and downstream processing. we can choose to drop other columns with null values or impute them based on the use case, but for now we will only drop rows with null values in symbol market_dt and dividend_ratio columns.
        
        # Replace null values
        df = df.fillna({
                "distr_freq": "Unknown", # will have to see about this column since it has a lot of null values. we can choose to drop this column if it has too many null values or if it is not useful for analysis and downstream processing, but for now we will replace null values with "Unknown".
                "payment_dt": None,
                "record_dt": None,
                "declar_dt": None
        })
        
        # Cast date+time columns to only date type (remove time component)
        df = df \
                .withColumn("market_dt",  col("market_dt").cast("date")) \
                .withColumn("payment_dt", col("payment_dt").cast("date")) \
                .withColumn("record_dt",  col("record_dt").cast("date")) \
                .withColumn("declar_dt",  col("declar_dt").cast("date")) \
                .withColumn("record_dt",  col("record_dt").cast("date"))
        
        # Reorganize columns
        df = df \
                .select(
                col("symbol"),
                col("market_dt"),
                col("dividend_ratio"),
                col("distr_freq"),
                col("payment_dt"),
                col("record_dt"),
                col("declar_dt")
        )
        
        # Write the transformed data back to GCS in delta lake format (parquet), partitioned by market_dt and clustered by symbol
        try:
                file_path = write_dividends_df_to_gcs(df, data_cat, tfd_bucket_nm, tfd_dir_nm, "market_dt", ["symbol", "market_dt"], tfd_file_type, tfd_save_mode, batch_dt, start_dt, end_dt)
        except Exception as e:
                return {"status": "error", "message": f"Error writing transformed data to GCS: {e}"}, 500
        
        return {"status": "success", "message": f"Data transformed and written to GCS at path: {file_path}"}, 200

