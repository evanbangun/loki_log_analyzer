import pandas as pd
import s3fs
import logging
import urllib3
import os
import socket
from botocore.config import Config
import pyarrow.dataset as ds
import pyarrow as pa
from pyspark.sql import SparkSession

# Disable SSL warnings if using self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    # Configure Spark
    spark = SparkSession.builder \
    .appName("MinIO Parquet Analysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://minio-api-sandbox-bigdata.layanan.go.id") \
    .config("spark.hadoop.fs.s3a.access.key", "jgaLNMQC6ownRRgrVIly") \
    .config("spark.hadoop.fs.s3a.secret.key", "bRFRU6pMfnwZthtxRFAFTmc7qL8avOEbuloUsb1L") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .getOrCreate()

    print(spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())

    print("Reading parquet files from MinIO...")
    df = spark.read.parquet("s3a://splp-logs/")

    print("Calculating average response latency by day...")
    df.groupBy("day").avg("responseLatency").show()

except Exception as e:
    print(e)
    print(f"Error type: {type(e).__name__}")
    raise
finally:
    if 'spark' in locals():
        spark.stop()