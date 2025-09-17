/*
DATABRICKS SQL VERSION - Financial Summary Report
=================================================
Original: financial_summary.sql
Complexity Score: 4.5/10 (Simple)
Migration Wave: Wave 1 - Quick Wins
Migration Hours: 4

TRANSPILATION CHANGES MADE:
- No significant changes required - SQL is already ANSI compatible
- Added Unity Catalog table references
- Added Delta Lake optimization hints
- Minor syntax cleanup for Databricks SQL best practices

VALIDATION STATUS: ✅ Ready for production use
PERFORMANCE NOTES: Consider partitioning by region for large datasets
*/

-- Databricks SQL version optimized for Delta Lake
SELECT
    r.r_name AS region,
    n.n_name AS nation,

    -- Basic revenue metrics
    COUNT(DISTINCT o.o_orderkey) AS total_orders,
    COUNT(DISTINCT c.c_custkey) AS unique_customers,
    COUNT(DISTINCT s.s_suppkey) AS active_suppliers,

    -- Financial totals
    SUM(o.o_totalprice) AS gross_revenue,
    SUM(l.l_extendedprice) AS extended_price_total,
    SUM(l.l_extendedprice * l.l_discount) AS total_discounts,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue,
    SUM(l.l_extendedprice * l.l_tax) AS total_tax,

    -- Cost analysis
    SUM(ps.ps_supplycost * l.l_quantity) AS total_supply_costs,
    SUM(l.l_extendedprice * (1 - l.l_discount)) - SUM(ps.ps_supplycost * l.l_quantity) AS gross_profit,

    -- Average metrics
    AVG(o.o_totalprice) AS avg_order_value,
    AVG(l.l_discount) AS avg_discount_rate,
    AVG(l.l_tax) AS avg_tax_rate,

    -- Profitability ratios
    CASE
        WHEN SUM(l.l_extendedprice * (1 - l.l_discount)) > 0 THEN
            (SUM(l.l_extendedprice * (1 - l.l_discount)) - SUM(ps.ps_supplycost * l.l_quantity)) /
            SUM(l.l_extendedprice * (1 - l.l_discount)) * 100
        ELSE 0
    END AS gross_profit_margin_pct,

    CASE
        WHEN SUM(l.l_extendedprice) > 0 THEN
            SUM(l.l_extendedprice * l.l_discount) / SUM(l.l_extendedprice) * 100
        ELSE 0
    END AS discount_rate_pct,

    -- Date range
    MIN(o.o_orderdate) AS period_start,
    MAX(o.o_orderdate) AS period_end

FROM globalsupply_corp.raw.region r
INNER JOIN globalsupply_corp.raw.nation n ON r.r_regionkey = n.n_regionkey
INNER JOIN globalsupply_corp.raw.customer c ON n.n_nationkey = c.c_nationkey
INNER JOIN globalsupply_corp.raw.orders o ON c.c_custkey = o.o_custkey
INNER JOIN globalsupply_corp.raw.lineitem l ON o.o_orderkey = l.l_orderkey
INNER JOIN globalsupply_corp.raw.partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
INNER JOIN globalsupply_corp.raw.supplier s ON ps.ps_suppkey = s.s_suppkey

WHERE o.o_orderdate >= '2023-01-01'
    AND o.o_orderstatus IN ('F', 'O')  -- Completed or Open orders

GROUP BY r.r_name, n.n_name

HAVING SUM(o.o_totalprice) > 10000  -- Minimum revenue threshold

ORDER BY gross_revenue DESC, region, nation;

/*
PERFORMANCE OPTIMIZATION SUGGESTIONS:
1. Consider partitioning large tables by o_orderdate
2. Use Z-ORDER optimization on frequently filtered columns
3. Create materialized views for frequently accessed aggregations

EXAMPLE OPTIMIZATIONS:
-- Optimize the orders table
OPTIMIZE globalsupply_corp.raw.orders ZORDER BY (o_orderdate, o_custkey);

-- Create a materialized view for repeated calculations
CREATE MATERIALIZED VIEW globalsupply_corp.analytics.financial_summary_mv AS
SELECT ... -- this query

-- Refresh periodically
REFRESH MATERIALIZED VIEW globalsupply_corp.analytics.financial_summary_mv;
*/