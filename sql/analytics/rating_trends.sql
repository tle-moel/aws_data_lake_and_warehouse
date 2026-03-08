SELECT 
    cat.product_category,
    d.year,
    ROUND(AVG(f.star_rating::DECIMAL(10,4)), 2) AS avg_rating,
    COUNT(*) AS review_count
FROM reviews.fact_reviews f
JOIN reviews.dim_category cat ON f.category_key = cat.category_key
JOIN reviews.dim_date d ON f.date_key = d.date_key
GROUP BY cat.product_category, d.year
ORDER BY cat.product_category, d.year;