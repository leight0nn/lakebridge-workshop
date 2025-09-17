/*
Supply Chain Performance Dashboard
===================================
Complexity Score: 8.5/10
Migration Hours: 16
Category: Analytics
SQL Features: CTEs, Window Functions, Complex Joins

Business Purpose:
Comprehensive supply chain performance analysis combining supplier metrics,
regional performance, and ranking analysis for executive dashboards.
*/

WITH regional_performance AS (
    SELECT
        r.r_name AS region_name,
        n.n_name AS nation_name,
        s.s_name AS supplier_name,
        s.s_suppkey,
        COUNT(DISTINCT o.o_orderkey) AS total_orders,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
        AVG(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) AS avg_ship_days,

        -- Window function for ranking
        ROW_NUMBER() OVER (
            PARTITION BY r.r_name
            ORDER BY SUM(l.l_extendedprice * (1 - l.l_discount)) DESC
        ) as revenue_rank,

        -- Running total within region
        SUM(SUM(l.l_extendedprice * (1 - l.l_discount))) OVER (
            PARTITION BY r.r_name
            ORDER BY SUM(l.l_extendedprice * (1 - l.l_discount)) DESC
            ROWS UNBOUNDED PRECEDING
        ) as running_revenue_total

    FROM region r
    INNER JOIN nation n ON r.r_regionkey = n.n_regionkey
    INNER JOIN supplier s ON n.n_nationkey = s.s_nationkey
    INNER JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    INNER JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE o.o_orderdate >= '2023-01-01'
        AND l.l_shipdate IS NOT NULL
    GROUP BY r.r_name, n.n_name, s.s_name, s.s_suppkey
),
performance_summary AS (
    SELECT
        region_name,
        COUNT(*) as supplier_count,
        SUM(revenue) as total_revenue,
        AVG(avg_ship_days) as region_avg_ship_days,

        -- Complex aggregation with window function
        MAX(CASE WHEN revenue_rank = 1 THEN supplier_name END) as top_supplier,
        MAX(CASE WHEN revenue_rank = 1 THEN revenue END) as top_supplier_revenue,

        -- Statistical functions
        STDEV(revenue) as revenue_std_dev,
        VAR(avg_ship_days) as shipping_variance

    FROM regional_performance
    GROUP BY region_name
)
SELECT
    ps.region_name,
    ps.supplier_count,
    ps.total_revenue,
    ps.region_avg_ship_days,
    ps.top_supplier,
    ps.top_supplier_revenue,
    ps.revenue_std_dev,
    ps.shipping_variance,

    -- Final ranking and percentiles
    RANK() OVER (ORDER BY ps.total_revenue DESC) as region_rank,
    PERCENT_RANK() OVER (ORDER BY ps.region_avg_ship_days) as shipping_performance_percentile,

    -- Performance categorization
    CASE
        WHEN ps.region_avg_ship_days <= 7 THEN 'Excellent'
        WHEN ps.region_avg_ship_days <= 14 THEN 'Good'
        WHEN ps.region_avg_ship_days <= 21 THEN 'Average'
        ELSE 'Needs Improvement'
    END as shipping_performance_category,

    -- Revenue contribution percentage
    ps.total_revenue / SUM(ps.total_revenue) OVER () * 100 as revenue_contribution_pct

FROM performance_summary ps
WHERE ps.total_revenue > 0
ORDER BY ps.total_revenue DESC;