-- Query 1: Total sales per product category
SELECT
    dp.product_category_name_english AS category,

    COUNT(f.order_id) AS order_count,
    SUM(f.total_price) AS total_sales,
    AVG(f.total_price) AS average_order_value
FROM
    dw.fact_order_items f
        JOIN
    dw.dim_products dp ON f.product_id = dp.product_id
GROUP BY
    dp.product_category_name_english,
    dp.product_category_name
ORDER BY
    total_sales DESC;
-- Query 2: Average delivery time per seller

SELECT
    avg(f.delivery_time)   AS avg_delivery,
    f.seller_id
FROM
    dw.fact_order_items f
        JOIN dw.dim_sellers ds ON f.seller_id = ds.seller_id
        JOIN dw.dim_date d ON f.delivery_date_id=d.date_id
GROUP BY f.seller_id
ORDER BY avg_delivery DESC

SELECT
    c.customer_state,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(DISTINCT c.customer_id) AS customer_count
FROM
    dw.fact_order_items f
JOIN
    dw.dim_customers c ON f.customer_id = c.customer_id
GROUP BY
    c.customer_state
ORDER BY
    order_count DESC;
