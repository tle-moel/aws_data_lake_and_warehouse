# AWS Data Lake & Warehouse — Amazon Customer Reviews

## Overview

This project implements an end-to-end data lake and warehouse pipeline on AWS, processing **13.8 million Amazon customer reviews** across three product categories: Electronics, Wireless, and Video Games.

Raw TSV files are ingested into S3, cleaned and transformed using PySpark on AWS Glue, and loaded into two analytical layers: a pre-aggregated analytics zone queryable via Athena, and a star schema in Redshift Serverless for structured reporting.

## Architecture

![Architecture Diagram](docs/architecture_diagram.svg)

## Technology Used

### Platform
- AWS (us-east-1)

### Services
- Amazon S3
- AWS Glue — schema discovery (Crawlers) and ETL jobs (PySpark)
- AWS Glue Data Catalog
- Amazon Athena
- Amazon Redshift Serverless

### Languages
- PySpark (ETL transformations)
- SQL (DDL, COPY commands, analytics queries)

## Data Used

The [Amazon US Customer Reviews dataset](https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset) is a public dataset containing product reviews from 1995 to 2015. The original AWS public S3 bucket (`s3://amazon-reviews-pds`) is no longer available — files were downloaded from Kaggle and uploaded manually to S3.

This project uses three categories:
- Electronics
- Wireless
- Video Games

## Data Model

The warehouse uses a star schema with one fact table and four dimension tables.

![Star Schema ERD](docs/star_schema_erd.png)

## Setup & Usage

### Prerequisites
- AWS account with IAM user configured (`aws configure`)
- IAM roles for Glue and Redshift (see [`infra/iam_roles.md`](infra/iam_roles.md))
- S3 bucket created in us-east-1

### 1 — Ingest raw data

Download the three TSV files from [Kaggle](https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset) and upload them to your S3 bucket:

```bash
aws s3 cp amazon_reviews_us_Electronics_v1_00.tsv.gz \
  s3://your-bucket/raw/product_category=Electronics/

aws s3 cp amazon_reviews_us_Wireless_v1_00.tsv.gz \
  s3://your-bucket/raw/product_category=Wireless/

aws s3 cp amazon_reviews_us_Video_Games_v1_00.tsv.gz \
  s3://your-bucket/raw/product_category=Video_Games/
```

### 2 — Run Glue ETL jobs

Upload the scripts in `/etl` to your Glue jobs and run them in order:

```
1. raw_to_curated.py          → writes cleaned Parquet to s3://your-bucket/curated/
2. curated_to_analytics.py    → writes aggregations to s3://your-bucket/analytics/
3. generate_dimensions.py     → writes star schema Parquet to s3://your-bucket/staging/redshift/
```

### 3 — Load Redshift

Create the schema and tables using `sql/ddl/create_tables.sql`, then load with:

```bash
sql/curated_to_redshift/load_star_schema.sql
```

### 4 — Run analytics queries

Queries are in `sql/analytics/` and results are in `docs/query_results/`

### Query 1 — Rating Trends by Category and Year

All three categories show a U-shaped rating trend — dipping in the early 2000s before recovering — with the smartphone era visible as Wireless review volume grows to 3M/year by 2015.

### Query 2 — Helpfulness Analysis per Category

Electronics reviews are the most helpful (70%) while Video Games are the least (56%), and only ~33% of all reviews ever received a helpfulness vote.

### Query 3 — Verified vs. Unverified Purchases

Verified purchases rate consistently higher across all categories, with the largest gap in Video Games (+0.38 stars) and the smallest in Wireless (+0.06).

### Query 4 — Vine Program Impact

Vine reviewers rate only marginally higher (+0.01 to +0.11 stars) despite receiving free products, suggesting the program's honest review requirement is largely respected. 

### Query 5 — Product Performance

AmazonBasics dominates the top 10 entirely with commodity accessories — review volume is driven by purchase frequency, not product quality, with the PS4 as the only non-accessory thanks to its 2013 launch spike.

## Future Enhancements

- Add **data freshness checks** and alerting via CloudWatch when the pipeline hasn't run within an expected window
- Introduce **incremental Glue job runs** using job bookmarks to process only new data rather than full reloads
- Add a **BI layer** on top of Redshift using Amazon QuickSight or an open-source alternative
- Extend to additional review categories to demonstrate the pipeline's scalability