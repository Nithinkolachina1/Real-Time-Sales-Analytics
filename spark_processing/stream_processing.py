from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, count, to_date, to_timestamp, collect_list, struct, slice
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

INPUT_PATH = f's3a://{BUCKET_NAME}/sales_stream/'
OUTPUT_PATH = f's3a://{BUCKET_NAME}/streaming_processed/'

schema = StructType([
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType())
])

spark = SparkSession.builder \
    .appName("RealTimeSalesBatchProcessor") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read all files in input path as batch (not streaming)
print(f"Input path is: {INPUT_PATH}")

df = spark.read.schema(schema).json(INPUT_PATH)

df = df.withColumn("timestamp", to_timestamp("timestamp")) \
       .withColumn("date", to_date("timestamp"))

daily_revenue = df.groupBy("date") \
    .agg(sum_("price").alias("total_revenue"))

top_products = df.groupBy("date", "category", "product_name") \
    .agg(count("*").alias("sale_count")) \
    .groupBy("date", "category") \
    .agg(
        collect_list(struct("product_name", "sale_count")).alias("top_products")
    ) \
    .select("date", "category", slice(col("top_products"), 1, 5).alias("top_5_products"))

daily_revenue.write.mode("overwrite").parquet(f"{OUTPUT_PATH}daily_revenue/")
top_products.write.mode("overwrite").parquet(f"{OUTPUT_PATH}top_products/")

spark.stop()
