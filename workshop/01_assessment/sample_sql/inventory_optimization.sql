/*
Inventory Optimization Analysis
===============================
Complexity Score: 9.2/10
Migration Hours: 24
Category: Analytics
SQL Features: Recursive CTEs, PIVOT, Advanced Analytics

Business Purpose:
Advanced inventory optimization using demand forecasting, ABC/XYZ classification,
and dynamic reorder point calculations with supplier lead time analysis.
*/

DECLARE @analysis_date DATE = '2024-01-01';

-- Recursive CTE for supplier hierarchy analysis
WITH supplier_hierarchy AS (
    -- Base case: Primary suppliers
    SELECT
        s.s_suppkey,
        s.s_name,
        s.s_nationkey,
        0 AS hierarchy_level,
        CAST(s.s_name AS VARCHAR(255)) AS supplier_path
    FROM supplier s
    WHERE s.s_suppkey IN (
        SELECT DISTINCT ps.ps_suppkey
        FROM partsupp ps
        GROUP BY ps.ps_suppkey
        HAVING COUNT(*) > 50  -- Major suppliers
    )

    UNION ALL

    -- Recursive case: Related suppliers in same nation
    SELECT
        s2.s_suppkey,
        s2.s_name,
        s2.s_nationkey,
        sh.hierarchy_level + 1,
        sh.supplier_path + ' -> ' + s2.s_name
    FROM supplier s2
    INNER JOIN supplier_hierarchy sh ON s2.s_nationkey = sh.s_nationkey
    WHERE sh.hierarchy_level < 2
        AND s2.s_suppkey != sh.s_suppkey
        AND s2.s_suppkey NOT IN (SELECT s_suppkey FROM supplier_hierarchy)
),
demand_analysis AS (
    SELECT
        p.p_partkey,
        p.p_name,
        p.p_category,
        ps.ps_availqty,
        ps.ps_supplycost,

        -- Demand calculations with window functions
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

        -- Demand variability
        STDEV(l.l_quantity) OVER (
            PARTITION BY l.l_partkey
            ORDER BY o.o_orderdate
            ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
        ) AS demand_std_dev,

        -- Lead time analysis
        AVG(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) OVER (
            PARTITION BY l.l_partkey, l.l_suppkey
        ) AS avg_lead_time,

        -- Inventory turnover calculation
        CASE
            WHEN ps.ps_availqty > 0 THEN
                SUM(l.l_quantity) OVER (PARTITION BY l.l_partkey) * 365.0 / ps.ps_availqty
            ELSE 0
        END AS inventory_turnover

    FROM part p
    LEFT JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
    LEFT JOIN lineitem l ON p.p_partkey = l.l_partkey
    LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE o.o_orderdate >= DATEADD(month, -12, @analysis_date)
),
abc_xyz_classification AS (
    SELECT
        p_partkey,
        p_name,
        p_category,
        ps_availqty,
        ps_supplycost,
        demand_90d,
        avg_demand_30d,
        demand_std_dev,
        avg_lead_time,
        inventory_turnover,

        -- ABC Classification using revenue contribution
        CASE NTILE(3) OVER (ORDER BY demand_90d * ps_supplycost DESC)
            WHEN 1 THEN 'A'
            WHEN 2 THEN 'B'
            ELSE 'C'
        END AS abc_class,

        -- XYZ Classification based on demand variability
        CASE
            WHEN ISNULL(demand_std_dev, 0) / NULLIF(avg_demand_30d, 0) < 0.5 THEN 'X'  -- Low variability
            WHEN ISNULL(demand_std_dev, 0) / NULLIF(avg_demand_30d, 0) < 1.5 THEN 'Y'  -- Medium variability
            ELSE 'Z'  -- High variability
        END AS xyz_class,

        -- Dynamic reorder point calculation
        CASE
            WHEN avg_demand_30d IS NOT NULL AND avg_lead_time IS NOT NULL THEN
                (avg_demand_30d * avg_lead_time) + (2 * ISNULL(demand_std_dev, 0) * SQRT(avg_lead_time))
            ELSE ps_availqty * 0.2  -- Fallback to 20% of current stock
        END AS calculated_reorder_point,

        -- Stock status determination
        CASE
            WHEN ps_availqty < (avg_demand_30d * 15) THEN 'CRITICAL'
            WHEN ps_availqty < (avg_demand_30d * 30) THEN 'LOW'
            WHEN ps_availqty > (avg_demand_30d * 90) THEN 'EXCESS'
            ELSE 'NORMAL'
        END AS stock_status

    FROM demand_analysis
    WHERE demand_90d IS NOT NULL
),
-- PIVOT table for monthly demand trends
monthly_demand_pivot AS (
    SELECT
        p_partkey,
        p_name,
        [1] AS jan_demand, [2] AS feb_demand, [3] AS mar_demand,
        [4] AS apr_demand, [5] AS may_demand, [6] AS jun_demand,
        [7] AS jul_demand, [8] AS aug_demand, [9] AS sep_demand,
        [10] AS oct_demand, [11] AS nov_demand, [12] AS dec_demand
    FROM (
        SELECT
            p.p_partkey,
            p.p_name,
            MONTH(o.o_orderdate) AS order_month,
            SUM(l.l_quantity) AS monthly_quantity
        FROM part p
        INNER JOIN lineitem l ON p.p_partkey = l.l_partkey
        INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
        WHERE o.o_orderdate >= DATEADD(year, -1, @analysis_date)
        GROUP BY p.p_partkey, p.p_name, MONTH(o.o_orderdate)
    ) AS source_table
    PIVOT (
        SUM(monthly_quantity)
        FOR order_month IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])
    ) AS pivot_table
)
SELECT
    abc.p_category,
    abc.abc_class + abc.xyz_class AS classification,
    abc.stock_status,
    COUNT(*) AS part_count,
    SUM(abc.ps_availqty * abc.ps_supplycost) AS inventory_value,
    AVG(abc.inventory_turnover) AS avg_turnover,
    AVG(abc.calculated_reorder_point) AS avg_reorder_point,

    -- Optimization recommendations using STRING_AGG (SQL Server specific)
    STRING_AGG(
        CASE
            WHEN abc.stock_status = 'CRITICAL' THEN abc.p_name
            ELSE NULL
        END,
        '; '
    ) WITHIN GROUP (ORDER BY abc.ps_availqty * abc.ps_supplycost DESC) AS critical_parts,

    -- Seasonal trend indicators
    AVG(CASE WHEN mdp.jan_demand > mdp.jul_demand THEN 1 ELSE 0 END) AS winter_bias_ratio,

    -- Economic order quantity approximation
    AVG(SQRT(2 * abc.avg_demand_30d * 30 * 100 / (abc.ps_supplycost * 0.25))) AS avg_eoq

FROM abc_xyz_classification abc
LEFT JOIN monthly_demand_pivot mdp ON abc.p_partkey = mdp.p_partkey
GROUP BY abc.p_category, abc.abc_class + abc.xyz_class, abc.stock_status
HAVING COUNT(*) >= 3  -- Only show meaningful segments
ORDER BY inventory_value DESC, part_count DESC;