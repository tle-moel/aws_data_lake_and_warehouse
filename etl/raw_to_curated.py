import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# --- Additional imports
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://tlm-amazon-reviews-lake/raw/"],
        "recurse": True
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": "\t"
    }
)

# Convert to standard PySpark DataFrame
df = dynamic_frame.toDF()

print(f"Raw row count: {df.count()}")

# --- Casting ---
df = df.withColumn("star_rating", F.col("star_rating").cast("int")) \
       .withColumn("review_date", F.col("review_date").cast("date")) \
       .withColumn("customer_id", F.col("customer_id").cast("string")) \
       .withColumn("verified_purchase", F.col("verified_purchase") == "Y") \
       .withColumn("vine", F.col("vine") == "Y") \
       .withColumn("helpful_votes", F.col("helpful_votes").cast("bigint")) \
       .withColumn("total_votes", F.col("total_votes").cast("bigint"))
       
# --- Null Handling ---
# Fill nulls with default values
df = df.fillna({
    "review_headline": "[No Title]",
    "helpful_votes": 0,
    "total_votes": 0,
    "verified_purchase": False,
    "vine": False
})

# Drop rows where critical columns are null
df = df.dropna(subset=[
    "review_id",
    "customer_id",
    "product_id",
    "star_rating",
    "review_body"
])

print(f"Row count after null drops: {df.count()}")

# --- Data Quality Filters ---
# Star rating must be between 1 and 5
df = df.filter(F.col("star_rating").between(1, 5))

# Non-negative votes
df = df.filter(F.col("helpful_votes") >= 0)
df = df.filter(F.col("total_votes") >= 0)

print(f"Row count after quality filters: {df.count()}")

# --- Deduplication ---
df = df.dropDuplicates(["review_id"])

print(f"Row count after deduplication: {df.count()}")

# --- Derived Columns ---
df = df.withColumn(
    "helpful_ratio",
    F.when(F.col("total_votes") > 0, F.col("helpful_votes") / F.col("total_votes")).otherwise(None)
)

# --- Metadata columns ---
df = df.withColumn("etl_processed_at", F.current_timestamp())

# --- Final Selection ---
df = df.select(
    # Identifiers
    "review_id",
    "customer_id",
    "product_id",
    "product_parent",
    "product_category",
    "product_title",
    # Review data
    "review_headline",
    "review_body",
    "review_date",
    # Measures
    "star_rating",
    "helpful_votes",
    "total_votes",
    "helpful_ratio",
    # Flags
    "verified_purchase",
    "vine",
    # Dimensions
    "marketplace",
    # Metadata
    "etl_processed_at"
)

print(f"Final row count to write: {df.count()}")

# Convert back and write to curated S3 as Parquet
output_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "output")
glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://tlm-amazon-reviews-lake/curated/",
                        "overwrite": "true"
    },
    format="parquet"
)

job.commit()