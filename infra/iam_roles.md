# IAM Roles

This document describes the IAM roles created manually via the AWS Console for this project.
No infrastructure-as-code was used — roles were set up once and reused across all phases.

---

## 1. Glue Service Role

**Purpose:** Allows AWS Glue (Crawlers and ETL Jobs) to read from and write to S3, and to manage the Glue Data Catalog.

**Attached policies:**

| Policy | Type | Reason |
|---|---|---|
| `AWSGlueServiceRole` | AWS managed | Grants Glue permission to manage jobs, crawlers, and the Data Catalog |
| `AmazonS3FullAccess` | AWS managed | Allows Glue to read from raw zone and write to curated, analytics, and staging zones |

---

## 2. Redshift S3 Access Role

**Purpose:** Allows Redshift Serverless to read Parquet files from the S3 staging zone using the `COPY` command.

**Attached policies:**

| Policy | Type | Reason |
|---|---|---|
| `AmazonRedshiftAllCommandsFullAccess` | AWS managed | Auto-attached by Redshift console; grants S3 read access for COPY commands |

---

## 3. IAM User (Daily Use)

**Purpose:** All AWS work was performed as an IAM user, never as root, following security best practices.

---

## Security Notes

- All S3 buckets have **public access blocked**
- Server-side encryption (**SSE-S3**) enabled on `tlm-amazon-reviews-lake`
- IAM role ARNs containing account IDs are redacted in all committed SQL files
- Billing alarms set at **$10** and **$30** via CloudWatch + SNS