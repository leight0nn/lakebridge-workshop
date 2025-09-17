/*
Dynamic Reporting System
========================
Complexity Score: 6.5/10
Migration Hours: 8
Category: Reporting
SQL Features: Dynamic SQL, Conditional Logic

Business Purpose:
Flexible reporting system that generates customized supply chain reports
based on user parameters with dynamic filtering and aggregation.
*/

-- Simulate stored procedure with dynamic SQL generation
DECLARE @report_type VARCHAR(50) = 'SUPPLIER_PERFORMANCE';
DECLARE @date_from DATE = '2023-01-01';
DECLARE @date_to DATE = '2023-12-31';
DECLARE @region_filter VARCHAR(100) = NULL;
DECLARE @min_order_value DECIMAL(18,2) = 1000.00;
DECLARE @include_details BIT = 1;
DECLARE @sql NVARCHAR(MAX) = '';

-- Dynamic SQL construction based on parameters
SET @sql = N'
WITH base_data AS (
    SELECT
        s.s_suppkey,
        s.s_name AS supplier_name,
        n.n_name AS nation,
        r.r_name AS region,
        COUNT(DISTINCT o.o_orderkey) AS order_count,
        SUM(l.l_quantity) AS total_quantity,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue,
        AVG(l.l_discount) AS avg_discount_rate,
        MIN(o.o_orderdate) AS first_order_date,
        MAX(o.o_orderdate) AS last_order_date
    FROM supplier s
    INNER JOIN nation n ON s.s_nationkey = n.n_nationkey
    INNER JOIN region r ON n.n_regionkey = r.r_regionkey
    INNER JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    INNER JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE 1=1';

-- Add date filters
IF @date_from IS NOT NULL
    SET @sql = @sql + N' AND o.o_orderdate >= @date_from';

IF @date_to IS NOT NULL
    SET @sql = @sql + N' AND o.o_orderdate <= @date_to';

-- Add region filter if specified
IF @region_filter IS NOT NULL
    SET @sql = @sql + N' AND r.r_name = @region_filter';

-- Add minimum order value filter
SET @sql = @sql + N' AND (l.l_extendedprice * (1 - l.l_discount)) >= @min_order_value';

-- Complete the base CTE
SET @sql = @sql + N'
    GROUP BY s.s_suppkey, s.s_name, n.n_name, r.r_name
),
performance_metrics AS (
    SELECT
        bd.*,
        net_revenue / NULLIF(order_count, 0) AS avg_order_value,
        total_quantity / NULLIF(order_count, 0) AS avg_quantity_per_order,
        DATEDIFF(day, first_order_date, last_order_date) AS active_period_days,

        -- Performance categories
        CASE
            WHEN net_revenue > (SELECT AVG(net_revenue) * 1.5 FROM base_data) THEN ''High Performer''
            WHEN net_revenue > (SELECT AVG(net_revenue) FROM base_data) THEN ''Average Performer''
            ELSE ''Developing''
        END AS performance_tier,

        -- Rank suppliers within their region
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY net_revenue DESC) AS regional_rank

    FROM base_data bd
)
SELECT
    region,';

-- Conditional field selection based on report type
IF @report_type = 'SUPPLIER_PERFORMANCE'
BEGIN
    SET @sql = @sql + N'
    supplier_name,
    nation,
    order_count,
    CAST(net_revenue AS DECIMAL(18,2)) AS net_revenue,
    CAST(avg_order_value AS DECIMAL(18,2)) AS avg_order_value,
    CAST(avg_discount_rate AS DECIMAL(5,4)) AS avg_discount_rate,
    performance_tier,
    regional_rank';
END
ELSE IF @report_type = 'REGIONAL_SUMMARY'
BEGIN
    SET @sql = @sql + N'
    COUNT(*) AS supplier_count,
    SUM(order_count) AS total_orders,
    CAST(SUM(net_revenue) AS DECIMAL(18,2)) AS total_revenue,
    CAST(AVG(avg_order_value) AS DECIMAL(18,2)) AS avg_order_value,
    CAST(AVG(avg_discount_rate) AS DECIMAL(5,4)) AS avg_discount_rate';
END
ELSE
BEGIN
    -- Default fields for unknown report types
    SET @sql = @sql + N'
    supplier_name,
    order_count,
    CAST(net_revenue AS DECIMAL(18,2)) AS net_revenue';
END

SET @sql = @sql + N'
FROM performance_metrics';

-- Add GROUP BY for regional summary
IF @report_type = 'REGIONAL_SUMMARY'
    SET @sql = @sql + N' GROUP BY region';

-- Add ORDER BY based on report type
IF @report_type = 'SUPPLIER_PERFORMANCE'
    SET @sql = @sql + N' ORDER BY region, net_revenue DESC';
ELSE IF @report_type = 'REGIONAL_SUMMARY'
    SET @sql = @sql + N' ORDER BY total_revenue DESC';
ELSE
    SET @sql = @sql + N' ORDER BY net_revenue DESC';

-- Optional: Add details section
IF @include_details = 1
BEGIN
    SET @sql = @sql + N'

    -- Additional detail query for drill-down analysis
    SELECT
        ''DETAIL_SECTION'' AS section_type,
        pm.supplier_name,
        pm.region,
        p.p_name AS top_product,
        SUM(l.l_quantity) AS product_quantity,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS product_revenue
    FROM performance_metrics pm
    INNER JOIN supplier s ON pm.supplier_name = s.s_name
    INNER JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    INNER JOIN part p ON ps.ps_partkey = p.p_partkey
    INNER JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE o.o_orderdate BETWEEN @date_from AND @date_to';

    IF @region_filter IS NOT NULL
        SET @sql = @sql + N' AND pm.region = @region_filter';

    SET @sql = @sql + N'
    GROUP BY pm.supplier_name, pm.region, p.p_name
    HAVING SUM(l.l_extendedprice * (1 - l.l_discount)) > @min_order_value
    ORDER BY pm.supplier_name, product_revenue DESC';
END

-- Execute the dynamic SQL (in real implementation)
-- EXEC sp_executesql @sql,
--     N'@date_from DATE, @date_to DATE, @region_filter VARCHAR(100), @min_order_value DECIMAL(18,2)',
--     @date_from, @date_to, @region_filter, @min_order_value;

-- For demonstration, show the generated SQL
PRINT '-- Generated Dynamic SQL for Report Type: ' + @report_type;
PRINT '-- Date Range: ' + CAST(@date_from AS VARCHAR) + ' to ' + CAST(@date_to AS VARCHAR);
IF @region_filter IS NOT NULL
    PRINT '-- Region Filter: ' + @region_filter;
PRINT '-- Minimum Order Value: ' + CAST(@min_order_value AS VARCHAR);
PRINT '-- Include Details: ' + CASE WHEN @include_details = 1 THEN 'Yes' ELSE 'No' END;
PRINT '';
PRINT @sql;

-- Alternative static version for immediate execution
WITH base_data AS (
    SELECT
        s.s_suppkey,
        s.s_name AS supplier_name,
        n.n_name AS nation,
        r.r_name AS region,
        COUNT(DISTINCT o.o_orderkey) AS order_count,
        SUM(l.l_quantity) AS total_quantity,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue,
        AVG(l.l_discount) AS avg_discount_rate,
        MIN(o.o_orderdate) AS first_order_date,
        MAX(o.o_orderdate) AS last_order_date
    FROM supplier s
    INNER JOIN nation n ON s.s_nationkey = n.n_nationkey
    INNER JOIN region r ON n.n_regionkey = r.r_regionkey
    INNER JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    INNER JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE o.o_orderdate >= '2023-01-01'
        AND o.o_orderdate <= '2023-12-31'
        AND (l.l_extendedprice * (1 - l.l_discount)) >= 1000.00
    GROUP BY s.s_suppkey, s.s_name, n.n_name, r.r_name
),
performance_metrics AS (
    SELECT
        bd.*,
        net_revenue / NULLIF(order_count, 0) AS avg_order_value,
        total_quantity / NULLIF(order_count, 0) AS avg_quantity_per_order,
        DATEDIFF(day, first_order_date, last_order_date) AS active_period_days,

        -- Performance categories using conditional logic
        CASE
            WHEN net_revenue > (SELECT AVG(net_revenue) * 1.5 FROM base_data) THEN 'High Performer'
            WHEN net_revenue > (SELECT AVG(net_revenue) FROM base_data) THEN 'Average Performer'
            ELSE 'Developing'
        END AS performance_tier,

        -- Dynamic ranking
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY net_revenue DESC) AS regional_rank,
        PERCENT_RANK() OVER (ORDER BY net_revenue) AS performance_percentile

    FROM base_data bd
)
SELECT
    region,
    supplier_name,
    nation,
    order_count,
    CAST(net_revenue AS DECIMAL(18,2)) AS net_revenue,
    CAST(avg_order_value AS DECIMAL(18,2)) AS avg_order_value,
    CAST(avg_discount_rate AS DECIMAL(5,4)) AS avg_discount_rate,
    performance_tier,
    regional_rank,
    CAST(performance_percentile * 100 AS DECIMAL(5,2)) AS performance_percentile_pct,
    active_period_days,

    -- Dynamic conditional formatting flags
    CASE WHEN regional_rank <= 3 THEN 'TOP_3_IN_REGION' ELSE NULL END AS highlight_flag,
    CASE WHEN avg_discount_rate > 0.1 THEN 'HIGH_DISCOUNT_ALERT' ELSE NULL END AS alert_flag

FROM performance_metrics
WHERE net_revenue > 0  -- Dynamic filter
ORDER BY region, net_revenue DESC;