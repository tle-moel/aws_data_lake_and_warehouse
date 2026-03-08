SELECT
    cat.product_category,
    ROUND(AVG(f.helpful_ratio::DECIMAL(10,4)), 4) AS avg_helpful_ratio,
    COUNT(*) AS review_count
FROM reviews.fact_reviews f
JOIN reviews.dim_category cat ON f.category_key = cat.category_key
WHERE CAST(f.total_votes AS INTEGER) > 0
GROUP BY cat.product_category
ORDER BY avg_helpful_ratio DESC;