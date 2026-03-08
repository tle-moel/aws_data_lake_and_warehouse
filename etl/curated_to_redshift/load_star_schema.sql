-- Replace <YOUR_ACCOUNT_ID> with your AWS account ID.
COPY reviews.dim_date
FROM 's3://tlm-amazon-reviews-lake/staging/redshift/dim_date/'
IAM_ROLE 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/AmazonRedshift-CommandsAccessRole-XXXXXXXXXXXXXXXXX'
FORMAT AS PARQUET;

COPY reviews.dim_category
FROM 's3://tlm-amazon-reviews-lake/staging/redshift/dim_category/'
IAM_ROLE 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/AmazonRedshift-CommandsAccessRole-XXXXXXXXXXXXXXXXX'
FORMAT AS PARQUET;

COPY reviews.dim_product
FROM 's3://tlm-amazon-reviews-lake/staging/redshift/dim_product/'
IAM_ROLE 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/AmazonRedshift-CommandsAccessRole-XXXXXXXXXXXXXXXXX'
FORMAT AS PARQUET;

COPY reviews.dim_customer
FROM 's3://tlm-amazon-reviews-lake/staging/redshift/dim_customer/'
IAM_ROLE 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/AmazonRedshift-CommandsAccessRole-XXXXXXXXXXXXXXXXX'
FORMAT AS PARQUET;

COPY reviews.fact_reviews
FROM 's3://tlm-amazon-reviews-lake/staging/redshift/fact_reviews/'
IAM_ROLE 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/AmazonRedshift-CommandsAccessRole-XXXXXXXXXXXXXXXXX'
FORMAT AS PARQUET;