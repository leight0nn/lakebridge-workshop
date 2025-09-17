/*
Financial Summary Report
=======================
Complexity Score: 4.5/10
Migration Hours: 4
Category: Reporting
SQL Features: Basic Aggregation, Simple Joins

Business Purpose:
Standard financial reporting with revenue summaries, cost analysis,
and basic profitability metrics for executive dashboards.
*/

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

FROM region r
INNER JOIN nation n ON r.r_regionkey = n.n_regionkey
INNER JOIN customer c ON n.n_nationkey = c.c_nationkey
INNER JOIN orders o ON c.c_custkey = o.o_custkey
INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
INNER JOIN partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
INNER JOIN supplier s ON ps.ps_suppkey = s.s_suppkey

WHERE o.o_orderdate >= '2023-01-01'
    AND o.o_orderstatus IN ('F', 'O')  -- Completed or Open orders

GROUP BY r.r_name, n.n_name

HAVING SUM(o.o_totalprice) > 10000  -- Minimum revenue threshold

ORDER BY gross_revenue DESC, region, nation;