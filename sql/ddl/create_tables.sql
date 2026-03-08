-- Create schema
CREATE TABLE reviews.dim_date (
    date_key        INTEGER PRIMARY KEY,
    review_date     TIMESTAMP,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    day_of_week     SMALLINT,
    quarter         SMALLINT
)
DISTSTYLE ALL
SORTKEY (date_key);

CREATE TABLE reviews.dim_category (
    category_key        INTEGER PRIMARY KEY,
    product_category    VARCHAR(50),
    marketplace         VARCHAR(5)
)
DISTSTYLE ALL
SORTKEY (category_key);

CREATE TABLE reviews.dim_product (
    product_key     INTEGER PRIMARY KEY,
    product_id      VARCHAR(50),
    product_parent  VARCHAR(50),
    product_title   VARCHAR(1000)
)
DISTSTYLE ALL
SORTKEY (product_key);

CREATE TABLE reviews.dim_customer (
    customer_key    INTEGER PRIMARY KEY,
    customer_id     VARCHAR(20)
)
DISTSTYLE ALL
SORTKEY (customer_key);

CREATE TABLE reviews.fact_reviews (
    review_id               VARCHAR(20),
    customer_key            INTEGER,
    product_key             INTEGER,
    date_key                INTEGER,
    category_key            INTEGER,
    star_rating             SMALLINT,
    helpful_votes           BIGINT,
    total_votes             BIGINT,
    helpful_ratio           FLOAT,
    verified_purchase_flag  BOOLEAN,
    vine_flag               BOOLEAN
)
DISTKEY (product_key)
SORTKEY (date_key, category_key);