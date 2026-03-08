import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Config ────────────────────────────────────────────────────────────
CURATED_PATH = "s3://tlm-amazon-reviews-lake/curated/"
STAGING_PATH = "s3://tlm-amazon-reviews-lake/staging/redshift/"

# ── Read curated Parquet ──────────────────────────────────────────────
df = spark.read.parquet(CURATED_PATH)
print(f"Total rows read: {df.count()}")

# ── dim_date ──────────────────────────────────────────────────────────
dim_date = df.select("review_date").distinct() \
    .filter(F.col("review_date").isNotNull()) \
    .withColumn("date_key",    F.date_format("review_date", "yyyyMMdd").cast("integer")) \
    .withColumn("year",        F.year("review_date").cast("smallint")) \
    .withColumn("month",       F.month("review_date").cast("smallint")) \
    .withColumn("day",         F.dayofmonth("review_date").cast("smallint")) \
    .withColumn("day_of_week", F.dayofweek("review_date").cast("smallint")) \
    .withColumn("quarter",     F.quarter("review_date").cast("smallint")) \
    .select("date_key", "review_date", "year", "month", "day", "day_of_week", "quarter")

dim_date.write.mode("overwrite").parquet(STAGING_PATH + "dim_date/")
print(f"dim_date rows: {dim_date.count()}")

# ── dim_category ──────────────────────────────────────────────────────
dim_category = df.select("product_category", "marketplace").distinct() \
    .filter(F.col("product_category").isNotNull()) \
    .withColumn("category_key", F.row_number().over(Window.orderBy("product_category"))) \
    .select("category_key", "product_category", "marketplace")

dim_category.write.mode("overwrite").parquet(STAGING_PATH + "dim_category/")
print(f"dim_category rows: {dim_category.count()}")

# ── dim_product ──────────────────────────────────────────────────────
dim_product = df.select("product_id", "product_parent", "product_title") \
    .filter(F.col("product_id").isNotNull()) \
    .groupBy("product_id") \
    .agg(
        F.first("product_parent").alias("product_parent"),
        F.first("product_title").alias("product_title")
    ) \
    .withColumn("product_key", F.row_number().over(Window.orderBy("product_id"))) \
    .select("product_key", "product_id", "product_parent", "product_title")

dim_product.write.mode("overwrite").parquet(STAGING_PATH + "dim_product/")
print(f"dim_product rows: {dim_product.count()}")

# ── dim_customer ──────────────────────────────────────────────────────
dim_customer = df.select("customer_id").distinct() \
    .filter(F.col("customer_id").isNotNull()) \
    .withColumn("customer_key", F.row_number().over(Window.orderBy("customer_id"))) \
    .select("customer_key", "customer_id")

dim_customer.write.mode("overwrite").parquet(STAGING_PATH + "dim_customer/")
print(f"dim_customer rows: {dim_customer.count()}")

# ── fact_reviews ──────────────────────────────────────────────────────
fact_reviews = df.filter(F.col("review_id").isNotNull()) \
    .join(dim_customer.select("customer_id", "customer_key"), on="customer_id", how="left") \
    .join(dim_product.select("product_id", "product_key"),   on="product_id",   how="left") \
    .join(dim_category.select("product_category", "category_key"), on="product_category", how="left") \
    .withColumn("date_key", F.date_format("review_date", "yyyyMMdd").cast("integer")) \
    .withColumn("verified_purchase_flag", F.col("verified_purchase").cast("boolean")) \
    .withColumn("vine_flag", F.col("vine").cast("boolean")) \
    .select(
        "review_id",
        "customer_key",
        "product_key",
        "date_key",
        "category_key",
        "star_rating",
        "helpful_votes",
        "total_votes",
        "helpful_ratio",
        "verified_purchase_flag",
        "vine_flag"
    )

fact_reviews.write.mode("overwrite").parquet(STAGING_PATH + "fact_reviews/")
print(f"fact_reviews rows: {fact_reviews.count()}")

job.commit()
print("Done — all staging files written to S3.")