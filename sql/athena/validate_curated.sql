-- Validation queries run in Amazon Athena after the Glue ETL

-- Preview a sample of curated records
SELECT * FROM curated LIMIT 10;

-- Check value ranges for key numeric and date columns
-- star_rating should be 1-5, helpful_ratio should be 0-1
SELECT 
    MIN(star_rating), MAX(star_rating),
    MIN(review_date), MAX(review_date),
    MIN(helpful_ratio), MAX(helpful_ratio)
FROM curated;

-- Confirm null review_body rows were dropped by the ETL job
-- Expected result: 0
SELECT COUNT(*) as null_review_bodies
FROM curated
WHERE review_body IS NULL;

-- Check boolean casting of verified_purchase (should be true/false, not Y/N)
SELECT verified_purchase, COUNT(*) 
FROM curated 
GROUP BY verified_purchase;

-- Investigate rows with null review_date (TSV column misalignment issue)
-- A small number of rows (~handful out of 13.8M) had misaligned columns
SELECT *
FROM curated
WHERE YEAR(review_date) IS NULL
LIMIT 10;