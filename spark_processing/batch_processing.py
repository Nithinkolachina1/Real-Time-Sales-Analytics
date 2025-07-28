from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

from pyspark.sql.functions import col, sum as sum_, count, window, to_date, to_timestamp, collect_list, struct, slice
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Replace with your actual bucket name and AWS credentials if needed
BUCKET_NAME=os.getenv("S3_BUCKET_NAME")
INPUT_PATH = f's3a://{BUCKET_NAME}/stream/'
OUTPUT_PATH = f's3a://{BUCKET_NAME}/processed/'
ACCESS_KEY=os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
# Create Spark session
spark = SparkSession.builder \
    .appName("SalesAnalytics") \
    .config("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def process_sales_data():
    # Define schema
    schema = StructType([
        StructField("order_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", StringType()),
    ])

    # Read JSON from S3 with schema
    df = spark.read.schema(schema).json(INPUT_PATH)
    df = df.withColumn("timestamp", to_timestamp("timestamp"))
    df = df.withColumn("date", to_date("timestamp"))

    # Daily revenue
    daily_revenue = df.groupBy("date") \
        .agg(sum_("price").alias("total_revenue")) \
        .orderBy("date")

    # Top 5 products by category per day
    top_products = df.groupBy("date", "category", "product_name") \
        .agg(count("*").alias("sale_count")) \
        .orderBy("date", "category", col("sale_count").desc()) \
        .groupBy("date", "category") \
        .agg(collect_list(struct("product_name", "sale_count")).alias("top_products")) \
        .select("date", "category", slice(col("top_products"), 1, 5).alias("top_5_products"))

    # Write to S3
    daily_revenue.write.mode("overwrite").parquet(f"{OUTPUT_PATH}daily_revenue/")
    top_products.write.mode("overwrite").parquet(f"{OUTPUT_PATH}top_products/")

    print("Processed data saved to S3")

if __name__ == "__main__":
    process_sales_data()
    spark.stop()
