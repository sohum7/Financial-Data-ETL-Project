from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from .utilities import getCurWkDtRange
from ..configs.config import MS_DIVIDENDS,MS_FILE_FORMAT, MS_DIVIDENDS_RAW_FILE_TYPE, MS_DIVIDENDS_RAW_FILE_BUCKET, MS_DIVIDENDS_RAW_FILE_BUCKET_SUBDIR, MS_DIVIDENDS_TFD_FILE_BUCKET, MS_DIVIDENDS_TFD_FILE_BUCKET_SUBDIR, MS_DIVIDENDS_TFD_FILE_TYPE

def main():
    spark = SparkSession.builder.appName(MS_DIVIDENDS).getOrCreate()
    monday, friday = getCurWkDtRange()

    # Read the raw JSON data from GCS into a Spark DataFrame
    blob_filename = MS_FILE_FORMAT(MS_DIVIDENDS, monday, friday, MS_DIVIDENDS_RAW_FILE_TYPE)
    df = spark.read.json(f"gs://{MS_DIVIDENDS_RAW_FILE_BUCKET}/{MS_DIVIDENDS_RAW_FILE_BUCKET_SUBDIR}{blob_filename}")

    # Flatten the nested JSON structure and select the relevant fields
    flattened_df = df.select(explode(col("data")).alias("record")).select("record.*")

    # Rename columns to match the desired schema
    col_name_change_df = flattened_df \
                            .withColumnRenamed("dividend", "dividend_ratio") \
                            .withColumnRenamed("date",  "market_dt") \
                            .withColumnRenamed("payment_date", "payment_dt") \
                            .withColumnRenamed("record_date",  "record_dt") \
                            .withColumnRenamed("declaration_date",  "declar_dt") \
                            .withColumnRenamed("record_date",  "record_dt")
                                    
    datatype_change_df = col_name_change_df \
                            .withColumn("market_dt",  col("market_dt").cast("date")) \
                            .withColumn("payment_dt", col("payment_dt").cast("date")) \
                            .withColumn("record_dt",  col("record_dt").cast("date")) \
                            .withColumn("declar_dt",  col("declar_dt").cast("date")) \
                            .withColumn("record_dt",  col("record_dt").cast("date"))
    # Reorganize the columns
    reorganized_col_df = datatype_change_df.select(
                            col("symbol"),
                            col("market_dt"),
                            col("dividend_ratio"),
                            col("distr_freq"),
                            col("payment_dt"),
                            col("record_dt"),
                            col("declar_dt")
    )

    # Write the transformed data back to GCS in delta lake format (parquet), partitioned by market_dt and clustered by symbol
    tfd_blob_filename = MS_FILE_FORMAT(MS_DIVIDENDS, monday, friday, MS_DIVIDENDS_TFD_FILE_TYPE)
    df.write.format(MS_DIVIDENDS_TFD_FILE_TYPE) \
        .partitionBy("market_dt") \
        .clusterBy("symbol") \
        .mode("overwrite") \
        .save(f"gs://{MS_DIVIDENDS_TFD_FILE_BUCKET}/{MS_DIVIDENDS_TFD_FILE_BUCKET_SUBDIR}delta/{blob_filename}")
