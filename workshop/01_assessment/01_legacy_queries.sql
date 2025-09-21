/*
GlobalSupply Corp - Legacy SQL Server Analytical Queries
========================================================

This file contains complex analytical queries from GlobalSupply Corp's legacy
SQL Server data warehouse. These queries represent typical supply chain analytics
that need to be migrated to Databricks.

Business Context:
- TPC-H style supply chain data (customers, orders, suppliers, parts, etc.)
- Complex analytical reporting requirements
- Performance-critical queries for business operations
*/

-- ==============================================================================
-- QUERY 1: Supply Chain Performance Dashboard
-- Complex multi-table joins with window functions and CTEs
-- ==============================================================================

WITH regional_performance AS (
    SELECT
        r.r_name AS region_name,
        n.n_name AS nation_name,
        s.s_name AS supplier_name,
        COUNT(DISTINCT o.o_orderkey) AS total_orders,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
        AVG(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) AS avg_ship_days,
        ROW_NUMBER() OVER (PARTITION BY r.r_name ORDER BY SUM(l.l_extendedprice * (1 - l.l_discount)) DESC) as revenue_rank
    FROM region r
    INNER JOIN nation n ON r.r_regionkey = n.n_regionkey
    INNER JOIN supplier s ON n.n_nationkey = s.s_nationkey
    INNER JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    INNER JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE o.o_orderdate >= '2023-01-01'

    
        AND l.l_shipdate IS NOT NULL
    GROUP BY r.r_name, n.n_name, s.s_name
),
performance_summary AS (
    SELECT
        region_name,
        COUNT(*) as supplier_count,
        SUM(revenue) as total_revenue,
        AVG(avg_ship_days) as region_avg_ship_days,
        MAX(CASE WHEN revenue_rank = 1 THEN supplier_name END) as top_supplier
    FROM regional_performance
    GROUP BY region_name
)
SELECT
    ps.*,
    RANK() OVER (ORDER BY total_revenue DESC) as region_rank,
    PERCENT_RANK() OVER (ORDER BY region_avg_ship_days) as shipping_performance_pct
FROM performance_summary ps
ORDER BY total_revenue DESC;

-- ==============================================================================
-- QUERY 2: Inventory Optimization Analysis
-- Uses SQL Server specific functions and complex aggregations
-- ==============================================================================

DECLARE @analysis_date DATE = '2024-01-01';

WITH inventory_metrics AS (
    SELECT
        p.p_partkey,
        p.p_name,
        p.p_category,
        p.p_brand,
        ps.ps_availqty,
        ps.ps_supplycost,

        -- Calculate demand using window functions
        SUM(l.l_quantity) OVER (
            PARTITION BY l.l_partkey
            ORDER BY o.o_orderdate
            ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
        ) AS demand_90d,

        AVG(l.l_quantity) OVER (
            PARTITION BY l.l_partkey
            ORDER BY o.o_orderdate
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS avg_demand_30d,

        -- Calculate inventory turnover
        CASE
            WHEN ps.ps_availqty > 0 THEN
                SUM(l.l_quantity) OVER (PARTITION BY l.l_partkey) * 365.0 / ps.ps_availqty
            ELSE 0
        END AS inventory_turnover,

        -- SQL Server specific date functions
        DATEDIFF(day,
            LAG(o.o_orderdate) OVER (PARTITION BY l.l_partkey ORDER BY o.o_orderdate),
            o.o_orderdate
        ) AS days_between_orders

    FROM part p
    LEFT JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
    LEFT JOIN lineitem l ON p.p_partkey = l.l_partkey
    LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE o.o_orderdate >= DATEADD(month, -12, @analysis_date)
),
inventory_classification AS (
    SELECT
        p_partkey,
        p_name,
        p_category,
        ps_availqty,
        ps_supplycost,
        demand_90d,
        avg_demand_30d,
        inventory_turnover,

        -- ABC Classification using NTILE
        NTILE(3) OVER (ORDER BY demand_90d DESC) AS abc_class,

        -- XYZ Classification based on demand variability
        CASE
            WHEN STDEV(avg_demand_30d) OVER (PARTITION BY p_partkey) < 0.5 THEN 'X'
            WHEN STDEV(avg_demand_30d) OVER (PARTITION BY p_partkey) < 1.5 THEN 'Y'
            ELSE 'Z'
        END AS xyz_class,

        -- Reorder recommendations
        CASE
            WHEN ps_availqty < (avg_demand_30d * 30) THEN 'URGENT_REORDER'
            WHEN ps_availqty < (avg_demand_30d * 45) THEN 'REORDER_SOON'
            WHEN ps_availqty > (avg_demand_30d * 90) THEN 'EXCESS_STOCK'
            ELSE 'OPTIMAL'
        END AS stock_status

    FROM inventory_metrics
    WHERE demand_90d IS NOT NULL
)
SELECT
    p_category,

    -- Convert ABC class number to letter
    CASE abc_class
        WHEN 1 THEN 'A'
        WHEN 2 THEN 'B'
        ELSE 'C'
    END AS abc_classification,

    xyz_class,
    stock_status,
    COUNT(*) AS part_count,
    SUM(ps_availqty * ps_supplycost) AS inventory_value,
    AVG(inventory_turnover) AS avg_turnover,

    -- String aggregation (SQL Server specific)
    STRING_AGG(
        CASE WHEN stock_status = 'URGENT_REORDER' THEN p_name ELSE NULL END,
        '; '
    ) AS urgent_reorder_parts

FROM inventory_classification
GROUP BY p_category, abc_class, xyz_class, stock_status
ORDER BY inventory_value DESC;

-- ==============================================================================
-- QUERY 3: Customer Profitability Analysis with Pivot
-- Uses SQL Server PIVOT and complex date calculations
-- ==============================================================================

WITH customer_metrics AS (
    SELECT
        c.c_custkey,
        c.c_name,
        c.c_acctbal,
        c.c_mktsegment,
        n.n_name AS customer_nation,
        r.r_name AS customer_region,

        -- Date calculations
        YEAR(o.o_orderdate) AS order_year,
        MONTH(o.o_orderdate) AS order_month,

        -- Revenue calculations
        SUM(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) AS gross_revenue,

        -- Order frequency metrics
        COUNT(DISTINCT o.o_orderkey) AS order_count,
        MIN(o.o_orderdate) AS first_order,
        MAX(o.o_orderdate) AS last_order,

        -- Product diversity
        COUNT(DISTINCT l.l_partkey) AS unique_products,

        -- Average order value
        AVG(l.l_extendedprice * (1 - l.l_discount)) AS avg_order_value

    FROM customer c
    INNER JOIN orders o ON c.c_custkey = o.o_custkey
    INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    INNER JOIN nation n ON c.c_nationkey = n.n_nationkey
    INNER JOIN region r ON n.n_regionkey = r.r_regionkey
    WHERE o.o_orderdate >= '2022-01-01'
    GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_mktsegment,
             n.n_name, r.r_name, YEAR(o.o_orderdate), MONTH(o.o_orderdate)
),
customer_yearly_revenue AS (
    SELECT
        c_custkey,
        c_name,
        c_mktsegment,
        customer_nation,
        customer_region,
        [2022] AS revenue_2022,
        [2023] AS revenue_2023,
        [2024] AS revenue_2024
    FROM (
        SELECT
            c_custkey,
            c_name,
            c_mktsegment,
            customer_nation,
            customer_region,
            order_year,
            SUM(gross_revenue) AS yearly_revenue
        FROM customer_metrics
        GROUP BY c_custkey, c_name, c_mktsegment, customer_nation,
                 customer_region, order_year
    ) AS source_table
    PIVOT (
        SUM(yearly_revenue)
        FOR order_year IN ([2022], [2023], [2024])
    ) AS pivot_table
),
customer_segments AS (
    SELECT
        *,
        -- Calculate growth rates
        CASE
            WHEN revenue_2022 > 0 THEN
                ((revenue_2023 - revenue_2022) / revenue_2022) * 100
            ELSE NULL
        END AS growth_2023,

        CASE
            WHEN revenue_2023 > 0 THEN
                ((revenue_2024 - revenue_2023) / revenue_2023) * 100
            ELSE NULL
        END AS growth_2024,

        -- Customer lifetime value (simplified)
        COALESCE(revenue_2022, 0) + COALESCE(revenue_2023, 0) + COALESCE(revenue_2024, 0) AS customer_ltv,

        -- RFM-like scoring
        CASE
            WHEN COALESCE(revenue_2024, 0) > 100000 THEN 'HIGH_VALUE'
            WHEN COALESCE(revenue_2024, 0) > 50000 THEN 'MEDIUM_VALUE'
            WHEN COALESCE(revenue_2024, 0) > 0 THEN 'LOW_VALUE'
            ELSE 'INACTIVE'
        END AS value_segment

    FROM customer_yearly_revenue
)
SELECT
    customer_region,
    c_mktsegment,
    value_segment,
    COUNT(*) AS customer_count,
    AVG(customer_ltv) AS avg_ltv,
    AVG(growth_2023) AS avg_growth_2023,
    AVG(growth_2024) AS avg_growth_2024,

    -- Top customer in segment
    (SELECT TOP 1 c_name
     FROM customer_segments cs2
     WHERE cs2.customer_region = cs.customer_region
       AND cs2.c_mktsegment = cs.c_mktsegment
       AND cs2.value_segment = cs.value_segment
     ORDER BY customer_ltv DESC) AS top_customer

FROM customer_segments cs
GROUP BY customer_region, c_mktsegment, value_segment
HAVING COUNT(*) >= 5  -- Only segments with meaningful size
ORDER BY avg_ltv DESC;

-- ==============================================================================
-- QUERY 4: Supplier Risk Assessment with Recursive CTE
-- Complex query using recursive CTEs and advanced analytics
-- ==============================================================================

WITH supplier_hierarchy AS (
    -- Recursive CTE to build supplier network relationships
    SELECT
        s.s_suppkey,
        s.s_name,
        s.s_nationkey,
        0 AS level,
        CAST(s.s_name AS VARCHAR(MAX)) AS hierarchy_path
    FROM supplier s
    WHERE s.s_suppkey IN (
        SELECT DISTINCT ps.ps_suppkey
        FROM partsupp ps
        GROUP BY ps.ps_suppkey
        HAVING COUNT(*) > 100  -- Major suppliers only
    )

    UNION ALL

    SELECT
        s2.s_suppkey,
        s2.s_name,
        s2.s_nationkey,
        sh.level + 1,
        sh.hierarchy_path + ' -> ' + s2.s_name
    FROM supplier s2
    INNER JOIN supplier_hierarchy sh ON s2.s_nationkey = sh.s_nationkey
    WHERE sh.level < 3  -- Limit recursion depth
        AND s2.s_suppkey != sh.s_suppkey
),
supplier_risk_metrics AS (
    SELECT
        s.s_suppkey,
        s.s_name,
        n.n_name AS supplier_nation,
        r.r_name AS supplier_region,

        -- Financial metrics
        COUNT(DISTINCT ps.ps_partkey) AS parts_supplied,
        AVG(ps.ps_supplycost) AS avg_supply_cost,
        SUM(ps.ps_availqty * ps.ps_supplycost) AS inventory_value,

        -- Performance metrics from orders
        COUNT(DISTINCT l.l_orderkey) AS orders_fulfilled,
        AVG(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) AS avg_fulfillment_days,

        -- Risk indicators
        CASE
            WHEN COUNT(DISTINCT ps.ps_partkey) = 1 THEN 5  -- Single product supplier
            WHEN COUNT(DISTINCT ps.ps_partkey) < 5 THEN 3
            ELSE 1
        END AS concentration_risk,

        -- Geographic risk (simplified)
        CASE r.r_name
            WHEN 'ASIA' THEN 3
            WHEN 'AMERICA' THEN 1
            WHEN 'EUROPE' THEN 2
            ELSE 4
        END AS geographic_risk,

        -- Performance risk
        CASE
            WHEN AVG(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) > 30 THEN 5
            WHEN AVG(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) > 14 THEN 3
            ELSE 1
        END AS performance_risk

    FROM supplier s
    INNER JOIN nation n ON s.s_nationkey = n.n_nationkey
    INNER JOIN region r ON n.n_regionkey = r.r_regionkey
    LEFT JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    LEFT JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey
    GROUP BY s.s_suppkey, s.s_name, n.n_name, r.r_name
),
risk_assessment AS (
    SELECT
        *,
        -- Composite risk score
        (concentration_risk + geographic_risk + performance_risk) AS total_risk_score,

        -- Risk categorization
        CASE
            WHEN (concentration_risk + geographic_risk + performance_risk) >= 12 THEN 'HIGH'
            WHEN (concentration_risk + geographic_risk + performance_risk) >= 8 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_category,

        -- Business criticality based on inventory value
        NTILE(4) OVER (ORDER BY inventory_value DESC) AS criticality_quartile

    FROM supplier_risk_metrics
)
SELECT
    supplier_region,
    risk_category,
    criticality_quartile,
    COUNT(*) AS supplier_count,
    AVG(total_risk_score) AS avg_risk_score,
    SUM(inventory_value) AS total_inventory_value,
    AVG(avg_fulfillment_days) AS avg_fulfillment,

    -- Mitigation recommendations using CASE
    CASE
        WHEN risk_category = 'HIGH' AND criticality_quartile IN (1,2) THEN 'URGENT: Diversify suppliers'
        WHEN risk_category = 'HIGH' THEN 'Monitor closely, develop contingency'
        WHEN risk_category = 'MEDIUM' AND criticality_quartile = 1 THEN 'Evaluate backup suppliers'
        ELSE 'Standard monitoring'
    END AS recommendation

FROM risk_assessment
GROUP BY supplier_region, risk_category, criticality_quartile
ORDER BY avg_risk_score DESC, total_inventory_value DESC;

-- ==============================================================================
-- QUERY 5: Dynamic SQL for Flexible Reporting (SQL Server Specific)
-- Demonstrates stored procedure-like functionality and dynamic SQL
-- ==============================================================================

DECLARE @sql NVARCHAR(MAX) = '';
DECLARE @date_filter VARCHAR(50) = '2023-01-01';
DECLARE @region_filter VARCHAR(100) = 'AMERICA';

-- Build dynamic query for flexible supply chain reporting
SET @sql = '
WITH supply_chain_metrics AS (
    SELECT
        r.r_name AS region,
        n.n_name AS nation,
        s.s_name AS supplier,
        p.p_category AS category,
        COUNT(DISTINCT o.o_orderkey) AS order_count,
        SUM(l.l_quantity) AS total_quantity,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue,
        AVG(l.l_discount) AS avg_discount
    FROM region r
    INNER JOIN nation n ON r.r_regionkey = n.n_regionkey
    INNER JOIN supplier s ON n.n_nationkey = s.s_nationkey
    INNER JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    INNER JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
    INNER JOIN part p ON l.l_partkey = p.p_partkey
    WHERE o.o_orderdate >= ''' + @date_filter + '''';

-- Add conditional filters
IF @region_filter IS NOT NULL
    SET @sql = @sql + ' AND r.r_name = ''' + @region_filter + '''';

SET @sql = @sql + '
    GROUP BY r.r_name, n.n_name, s.s_name, p.p_category
)
SELECT
    region,
    category,
    COUNT(*) AS supplier_count,
    SUM(order_count) AS total_orders,
    SUM(net_revenue) AS total_revenue,
    AVG(avg_discount) AS avg_discount_rate,
    -- Performance indicators
    CASE
        WHEN SUM(net_revenue) > 1000000 THEN ''High Performer''
        WHEN SUM(net_revenue) > 500000 THEN ''Medium Performer''
        ELSE ''Developing''
    END AS performance_tier
FROM supply_chain_metrics
GROUP BY region, category
ORDER BY total_revenue DESC';

-- Execute dynamic SQL (in real scenario)
-- EXEC sp_executesql @sql;

-- For assessment purposes, show the generated SQL
PRINT '-- Generated Dynamic SQL:';
PRINT @sql;

-- ==============================================================================
-- QUERY 6: Advanced Window Functions and Ranking
-- Complex analytical query with multiple window functions
-- ==============================================================================

SELECT
    c.c_name AS customer_name,
    c.c_mktsegment,
    o.o_orderdate,
    l.l_extendedprice * (1 - l.l_discount) AS order_value,

    -- Running totals and moving averages
    SUM(l.l_extendedprice * (1 - l.l_discount)) OVER (
        PARTITION BY c.c_custkey
        ORDER BY o.o_orderdate
        ROWS UNBOUNDED PRECEDING
    ) AS running_total,

    AVG(l.l_extendedprice * (1 - l.l_discount)) OVER (
        PARTITION BY c.c_custkey
        ORDER BY o.o_orderdate
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_orders,

    -- Rankings and percentiles
    RANK() OVER (
        PARTITION BY YEAR(o.o_orderdate), c.c_mktsegment
        ORDER BY l.l_extendedprice * (1 - l.l_discount) DESC
    ) AS yearly_segment_rank,

    PERCENT_RANK() OVER (
        ORDER BY l.l_extendedprice * (1 - l.l_discount)
    ) AS percentile_rank,

    -- Lead/Lag for trend analysis
    LAG(l.l_extendedprice * (1 - l.l_discount), 1) OVER (
        PARTITION BY c.c_custkey
        ORDER BY o.o_orderdate
    ) AS previous_order_value,

    LEAD(o.o_orderdate, 1) OVER (
        PARTITION BY c.c_custkey
        ORDER BY o.o_orderdate
    ) AS next_order_date,

    -- First and last values
    FIRST_VALUE(l.l_extendedprice * (1 - l.l_discount)) OVER (
        PARTITION BY c.c_custkey
        ORDER BY o.o_orderdate
        ROWS UNBOUNDED PRECEDING
    ) AS first_order_value,

    LAST_VALUE(l.l_extendedprice * (1 - l.l_discount)) OVER (
        PARTITION BY c.c_custkey
        ORDER BY o.o_orderdate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_order_value

FROM customer c
INNER JOIN orders o ON c.c_custkey = o.o_custkey
INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderdate >= '2023-01-01'
    AND l.l_extendedprice * (1 - l.l_discount) > 1000
ORDER BY c.c_name, o.o_orderdate;