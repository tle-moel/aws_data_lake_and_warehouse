SELECT
    cat.product_category,
    p.product_id,
    p.product_title,
    COUNT(*) AS review_count,
    ROUND(AVG(f.star_rating::DECIMAL(10,4)), 2) AS avg_rating
FROM reviews.fact_reviews f
JOIN reviews.dim_category cat ON f.category_key = cat.category_key
JOIN reviews.dim_product p ON f.product_key = p.product_key
GROUP BY cat.product_category, p.product_id, p.product_title
ORDER BY review_count DESC
LIMIT 10;