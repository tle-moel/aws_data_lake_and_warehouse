import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CURATED_PATH = "s3://tlm-amazon-reviews-lake/curated/"
ANALYTICS_PATH = "s3://tlm-amazon-reviews-lake/analytics/"

# Read curated Parquet
df = spark.read.parquet(CURATED_PATH)

# Extract year from review_date
df = df.withColumn("year", F.year("review_date"))

# --- Agg 1: Rating trends by category and year ---
agg1 = df.groupBy("product_category", "year") \
    .agg(
        F.round(F.avg("star_rating"), 2).alias("avg_star_rating"),
        F.count("review_id").alias("review_count")
    )
agg1.write.mode("overwrite").parquet(f"{ANALYTICS_PATH}agg_rating_by_category_year/")

# --- Agg 2: Helpfulness by category ---
agg2 = df.groupBy("product_category") \
    .agg(
        F.round(F.avg("helpful_ratio"), 2).alias("avg_helpful_ratio"),
        F.count("review_id").alias("review_count")
    )
agg2.write.mode("overwrite").parquet(f"{ANALYTICS_PATH}agg_helpfulness_by_category/")

# --- Agg 3: Verified vs unverified ---
agg3 = df.groupBy("product_category", "verified_purchase") \
    .agg(
        F.round(F.avg("star_rating"),2).alias("avg_star_rating"),
        F.count("review_id").alias("review_count")
    )
agg3.write.mode("overwrite").parquet(f"{ANALYTICS_PATH}agg_verified_vs_unverified/")

# --- Agg 4: Vine vs non-Vine ---
agg4 = df.groupBy("product_category", "vine") \
    .agg(
        F.round(F.avg("star_rating"),2).alias("avg_star_rating"),
        F.count("review_id").alias("review_count")
    )
agg4.write.mode("overwrite").parquet(f"{ANALYTICS_PATH}agg_vine_vs_non_vine/")

# --- Agg 5: Top products by category ---
agg5 = df.groupBy("product_category", "product_id", "product_title") \
    .agg(
        F.count("review_id").alias("review_count"),
        F.round(F.avg("star_rating"),2).alias("avg_star_rating")
    )
agg5.write.mode("overwrite").parquet(f"{ANALYTICS_PATH}agg_top_products/")

job.commit()