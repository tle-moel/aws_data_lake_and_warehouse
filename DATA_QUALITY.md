# Data Quality

This document describes the data quality rules applied during the ETL transformation (`raw_to_curated.py`) and the findings discovered during validation in Athena.

---

## Transformation Rules

### Type Casting

| Column | Raw Type | Curated Type | Reason |
|---|---|---|---|
| `star_rating` | string | int | TSV ingestion reads all columns as string; cast needed for aggregations |
| `review_date` | string | date | Enables date functions and time-based filtering |
| `customer_id` | bigint | string | Identifier, not a number — string is semantically correct |
| `verified_purchase` | string (Y/N) | boolean | Normalised to true/false |
| `vine` | string (Y/N) | boolean | Normalised to true/false |
| `helpful_votes` | string | bigint | TSV ingestion reads as string; cast needed for arithmetic |
| `total_votes` | string | bigint | TSV ingestion reads as string; cast needed for arithmetic |

### Null Handling

| Column | Rule |
|---|---|
| `review_headline` | Filled with `[No Title]` |
| `helpful_votes` | Filled with `0` |
| `total_votes` | Filled with `0` |
| `verified_purchase` | Filled with `False` |
| `vine` | Filled with `False` |
| `review_id`, `customer_id`, `product_id`, `star_rating`, `review_body` | Rows dropped if null |

### Data Quality Filters

- `star_rating` must be between 1 and 5 (inclusive)
- `helpful_votes` must be >= 0
- `total_votes` must be >= 0

### Deduplication

Rows deduplicated on `review_id`.

### Derived Column

`helpful_ratio = helpful_votes / total_votes` — computed only when `total_votes > 0`, otherwise `null`. Avoids divide-by-zero errors.

---

## Storage Compression

| Zone | Format | Size |
|---|---|---|
| Raw | TSV (gzipped) | 6.6 GB |
| Curated | Parquet | 3.4 GB |
| Reduction | | **~48%** |

Switching from TSV to Parquet reduced storage by nearly half. In addition to cost savings, Parquet's columnar format means analytical queries scan only the columns they need, reducing Athena query costs significantly.

---

## Known Issues & Anomalies

### TSV Column Misalignment
A small number of rows (~handful out of 13.8M) had misaligned columns, resulting in null review dates. These rows were kept but are visible in validation queries.

### Partitioning Decision
Hive-style partitioning by `product_category` was skipped in the curated zone to avoid a naming conflict with the existing column in the source files. The curated zone uses a flat Parquet layout.

---