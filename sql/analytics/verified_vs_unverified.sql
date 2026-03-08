SELECT
    cat.product_category,
    f.verified_purchase_flag,
    ROUND(AVG(f.star_rating::DECIMAL(10,4)), 2) AS avg_rating,
    COUNT(*) AS review_count
FROM reviews.fact_reviews f
JOIN reviews.dim_category cat ON f.category_key = cat.category_key
GROUP BY cat.product_category, f.verified_purchase_flag
ORDER BY cat.product_category, f.verified_purchase_flag;