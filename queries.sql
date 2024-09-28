
---




```sql
-- 1. Countries We Operate In and the Country with the Most Stores
SELECT
    country_code AS country,
    COUNT(*) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_no_stores DESC;

-- 2. Locations with the Most Stores
SELECT
    locality,
    COUNT(*) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC
LIMIT 7;

-- 3. Months with the Most Sales
SELECT
    SUM(od.product_quantity * dp.product_price) AS total_sales,
    dd.month
FROM
    orders_table od
JOIN
    dim_date_times dd ON od.date_uuid = dd.date_uuid
JOIN
    dim_products dp ON od.product_code = dp.product_code
GROUP BY
    dd.month
ORDER BY
    total_sales DESC
LIMIT 6;

-- 4. Online vs. Offline Sales
SELECT
    COUNT(*) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE
        WHEN od.store_code = 'WEB' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table od
GROUP BY
    location
ORDER BY
    location;

-- 5. Sales by Store Type with Percentages
WITH total_sales_cte AS (
    SELECT
        ds.store_type,
        SUM(od.product_quantity * dp.product_price) AS total_sales
    FROM
        orders_table od
    JOIN
        dim_store_details ds ON od.store_code = ds.store_code
    JOIN
        dim_products dp ON od.product_code = dp.product_code
    GROUP BY
        ds.store_type
), grand_total AS (
    SELECT
        SUM(total_sales) AS grand_total_sales
    FROM
        total_sales_cte
)
SELECT
    ts.store_type,
    ts.total_sales,
    ROUND((ts.total_sales / gt.grand_total_sales) * 100, 2) AS percentage_total
FROM
    total_sales_cte ts, grand_total gt
ORDER BY
    ts.total_sales DESC;

-- 6. Historical Sales by Month and Year
SELECT
    SUM(od.product_quantity * dp.product_price) AS total_sales,
    dd.year,
    dd.month
FROM
    orders_table od
JOIN
    dim_date_times dd ON od.date_uuid = dd.date_uuid
JOIN
    dim_products dp ON od.product_code = dp.product_code
GROUP BY
    dd.year, dd.month
ORDER BY
    total_sales DESC
LIMIT 10;

-- 7. Staff Numbers by Country
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;

-- 8. Store Types Generating Most Sales in Germany
SELECT
    SUM(od.product_quantity * dp.product_price) AS total_sales,
    ds.store_type,
    ds.country_code
FROM
    orders_table od
JOIN
    dim_store_details ds ON od.store_code = ds.store_code
JOIN
    dim_products dp ON od.product_code = dp.product_code
WHERE
    ds.country_code = 'DE'
GROUP BY
    ds.store_type, ds.country_code
ORDER BY
    total_sales DESC;

-- 9. Average Time Taken Between Sales Grouped by Year
WITH sales_times AS (
    SELECT
        dd.year,
        od.created_at
    FROM
        orders_table od
    JOIN
        dim_date_times dd ON od.date_uuid = dd.date_uuid
    ORDER BY
        od.created_at
), time_differences AS (
    SELECT
        year,
        created_at,
        LEAD(created_at) OVER (PARTITION BY year ORDER BY created_at) AS next_created_at
    FROM
        sales_times
)
SELECT
    year,
    MAKE_INTERVAL(secs => AVG(EXTRACT(EPOCH FROM (next_created_at - created_at))))
        AS actual_time_taken
FROM
    time_differences
WHERE
    next_created_at IS NOT NULL
GROUP BY
    year
ORDER BY
    year;
