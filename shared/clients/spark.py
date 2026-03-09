from pyspark.sql import SparkSession

def read_json_from_gcs(file_path, logger=None):
    spark = SparkSession.builder.appName("DividendsTransform").getOrCreate()
    return spark.read.json(file_path)