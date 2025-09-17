/*
Supplier Risk Assessment
========================
Complexity Score: 9.8/10
Migration Hours: 32
Category: Analytics
SQL Features: Recursive CTEs, Dynamic SQL, Risk Scoring

Business Purpose:
Advanced supplier risk assessment combining financial metrics, geographic risk,
performance indicators, and network effect analysis for supply chain resilience.
*/

-- Recursive CTE for supplier network analysis
WITH supplier_network AS (
    -- Base case: Primary suppliers
    SELECT
        s.s_suppkey,
        s.s_name,
        s.s_nationkey,
        s.s_acctbal,
        0 AS network_level,
        CAST(s.s_name AS VARCHAR(MAX)) AS supply_chain_path,
        s.s_suppkey AS root_supplier
    FROM supplier s
    WHERE s.s_suppkey IN (
        SELECT DISTINCT ps.ps_suppkey
        FROM partsupp ps
        INNER JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
        GROUP BY ps.ps_suppkey
        HAVING COUNT(DISTINCT ps.ps_partkey) > 20  -- Strategic suppliers
    )

    UNION ALL

    -- Recursive case: Connected suppliers through shared parts or regions
    SELECT
        s2.s_suppkey,
        s2.s_name,
        s2.s_nationkey,
        s2.s_acctbal,
        sn.network_level + 1,
        sn.supply_chain_path + ' -> ' + s2.s_name,
        sn.root_supplier
    FROM supplier s2
    INNER JOIN supplier_network sn ON (
        s2.s_nationkey = sn.s_nationkey OR  -- Same nation
        EXISTS (
            SELECT 1
            FROM partsupp ps1
            INNER JOIN partsupp ps2 ON ps1.ps_partkey = ps2.ps_partkey
            WHERE ps1.ps_suppkey = sn.s_suppkey
            AND ps2.ps_suppkey = s2.s_suppkey
        )
    )
    WHERE sn.network_level < 3
        AND s2.s_suppkey != sn.s_suppkey
        AND s2.s_suppkey NOT IN (
            SELECT s_suppkey FROM supplier_network WHERE root_supplier = sn.root_supplier
        )
),
supplier_performance_metrics AS (
    SELECT
        s.s_suppkey,
        s.s_name,
        n.n_name AS supplier_nation,
        r.r_name AS supplier_region,
        s.s_acctbal AS financial_balance,

        -- Performance metrics
        COUNT(DISTINCT ps.ps_partkey) AS parts_supplied,
        AVG(ps.ps_supplycost) AS avg_supply_cost,
        SUM(ps.ps_availqty * ps.ps_supplycost) AS inventory_value,
        STDEV(ps.ps_supplycost) AS cost_volatility,

        -- Delivery performance
        COUNT(DISTINCT l.l_orderkey) AS orders_fulfilled,
        AVG(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) AS avg_lead_time,
        STDEV(DATEDIFF(day, o.o_orderdate, l.l_shipdate)) AS lead_time_variability,

        -- Quality indicators (return rates)
        CAST(SUM(CASE WHEN l.l_returnflag = 'R' THEN l.l_quantity ELSE 0 END) AS FLOAT) /
        NULLIF(SUM(l.l_quantity), 0) * 100 AS return_rate_pct,

        -- Financial stability indicators
        CASE
            WHEN s.s_acctbal < 0 THEN 5  -- Negative balance = high risk
            WHEN s.s_acctbal < 1000 THEN 4
            WHEN s.s_acctbal < 5000 THEN 3
            WHEN s.s_acctbal < 10000 THEN 2
            ELSE 1
        END AS financial_risk_score

    FROM supplier s
    INNER JOIN nation n ON s.s_nationkey = n.n_nationkey
    INNER JOIN region r ON n.n_regionkey = r.r_regionkey
    LEFT JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
    LEFT JOIN lineitem l ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE o.o_orderdate >= DATEADD(year, -2, GETDATE()) OR o.o_orderdate IS NULL
    GROUP BY s.s_suppkey, s.s_name, n.n_name, r.r_name, s.s_acctbal
),
geographic_risk_analysis AS (
    SELECT
        spm.*,
        -- Geographic risk scoring
        CASE
            WHEN supplier_region = 'MIDDLE EAST' THEN 5  -- Highest geopolitical risk
            WHEN supplier_region = 'AFRICA' THEN 4
            WHEN supplier_region = 'ASIA' THEN 3
            WHEN supplier_region = 'EUROPE' THEN 2
            WHEN supplier_region = 'AMERICA' THEN 1
            ELSE 3
        END AS geographic_risk_score,

        -- Concentration risk within region
        COUNT(*) OVER (PARTITION BY supplier_region) AS suppliers_in_region,
        SUM(inventory_value) OVER (PARTITION BY supplier_region) AS region_total_value,
        inventory_value / SUM(inventory_value) OVER () * 100 AS supplier_value_concentration_pct

    FROM supplier_performance_metrics spm
),
performance_risk_scoring AS (
    SELECT
        gra.*,
        -- Performance risk components
        CASE
            WHEN avg_lead_time > 30 THEN 5
            WHEN avg_lead_time > 21 THEN 4
            WHEN avg_lead_time > 14 THEN 3
            WHEN avg_lead_time > 7 THEN 2
            ELSE 1
        END AS lead_time_risk_score,

        CASE
            WHEN ISNULL(lead_time_variability, 0) > 10 THEN 5
            WHEN ISNULL(lead_time_variability, 0) > 7 THEN 4
            WHEN ISNULL(lead_time_variability, 0) > 5 THEN 3
            WHEN ISNULL(lead_time_variability, 0) > 3 THEN 2
            ELSE 1
        END AS variability_risk_score,

        CASE
            WHEN return_rate_pct > 10 THEN 5
            WHEN return_rate_pct > 7 THEN 4
            WHEN return_rate_pct > 5 THEN 3
            WHEN return_rate_pct > 2 THEN 2
            ELSE 1
        END AS quality_risk_score,

        CASE
            WHEN parts_supplied = 1 THEN 5  -- Single product dependency
            WHEN parts_supplied <= 3 THEN 4
            WHEN parts_supplied <= 5 THEN 3
            WHEN parts_supplied <= 10 THEN 2
            ELSE 1
        END AS concentration_risk_score

    FROM geographic_risk_analysis gra
),
network_effects AS (
    SELECT
        prs.s_suppkey,
        prs.s_name,
        COUNT(sn.s_suppkey) AS network_connections,
        AVG(sn.network_level) AS avg_network_distance,
        MAX(sn.network_level) AS max_network_depth

    FROM performance_risk_scoring prs
    LEFT JOIN supplier_network sn ON prs.s_suppkey = sn.root_supplier OR prs.s_suppkey = sn.s_suppkey
    GROUP BY prs.s_suppkey, prs.s_name
),
comprehensive_risk_assessment AS (
    SELECT
        prs.*,
        ne.network_connections,
        ne.avg_network_distance,
        ne.max_network_depth,

        -- Composite risk score calculation
        (financial_risk_score + geographic_risk_score + lead_time_risk_score +
         variability_risk_score + quality_risk_score + concentration_risk_score) AS total_risk_score,

        -- Network resilience factor (more connections = lower risk)
        CASE
            WHEN ne.network_connections >= 10 THEN -1  -- Risk reduction
            WHEN ne.network_connections >= 5 THEN 0
            ELSE 1  -- Risk increase
        END AS network_risk_adjustment,

        -- Business criticality (based on inventory value and parts supplied)
        CASE
            WHEN prs.inventory_value > (SELECT AVG(inventory_value) * 2 FROM performance_risk_scoring) AND prs.parts_supplied > 10 THEN 'CRITICAL'
            WHEN prs.inventory_value > (SELECT AVG(inventory_value) FROM performance_risk_scoring) OR prs.parts_supplied > 5 THEN 'IMPORTANT'
            ELSE 'STANDARD'
        END AS business_criticality

    FROM performance_risk_scoring prs
    LEFT JOIN network_effects ne ON prs.s_suppkey = ne.s_suppkey
),
risk_categorization AS (
    SELECT
        *,
        -- Final adjusted risk score
        total_risk_score + network_risk_adjustment AS adjusted_risk_score,

        -- Risk category determination
        CASE
            WHEN (total_risk_score + network_risk_adjustment) >= 20 THEN 'EXTREME'
            WHEN (total_risk_score + network_risk_adjustment) >= 15 THEN 'HIGH'
            WHEN (total_risk_score + network_risk_adjustment) >= 10 THEN 'MEDIUM'
            WHEN (total_risk_score + network_risk_adjustment) >= 5 THEN 'LOW'
            ELSE 'MINIMAL'
        END AS risk_category

    FROM comprehensive_risk_assessment
)
SELECT
    supplier_region,
    risk_category,
    business_criticality,
    COUNT(*) AS supplier_count,
    AVG(adjusted_risk_score) AS avg_risk_score,
    SUM(inventory_value) AS total_inventory_value,
    AVG(avg_lead_time) AS avg_lead_time,
    AVG(return_rate_pct) AS avg_return_rate,
    AVG(supplier_value_concentration_pct) AS avg_concentration,

    -- Mitigation recommendations using CASE statements
    CASE
        WHEN risk_category = 'EXTREME' AND business_criticality = 'CRITICAL' THEN 'URGENT: Immediate supplier diversification required'
        WHEN risk_category = 'EXTREME' THEN 'HIGH PRIORITY: Develop alternative suppliers within 30 days'
        WHEN risk_category = 'HIGH' AND business_criticality = 'CRITICAL' THEN 'Evaluate backup suppliers and increase safety stock'
        WHEN risk_category = 'HIGH' THEN 'Monitor closely, develop contingency plans'
        WHEN risk_category = 'MEDIUM' AND business_criticality = 'CRITICAL' THEN 'Assess alternative sourcing options'
        ELSE 'Standard monitoring and periodic review'
    END AS mitigation_strategy,

    -- Financial impact estimation
    SUM(inventory_value) *
    CASE
        WHEN risk_category = 'EXTREME' THEN 0.15  -- 15% potential loss
        WHEN risk_category = 'HIGH' THEN 0.08     -- 8% potential loss
        WHEN risk_category = 'MEDIUM' THEN 0.03   -- 3% potential loss
        ELSE 0.01                                 -- 1% potential loss
    END AS estimated_financial_risk

FROM risk_categorization
GROUP BY supplier_region, risk_category, business_criticality
HAVING COUNT(*) >= 1
ORDER BY
    CASE risk_category
        WHEN 'EXTREME' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
        ELSE 5
    END,
    CASE business_criticality
        WHEN 'CRITICAL' THEN 1
        WHEN 'IMPORTANT' THEN 2
        ELSE 3
    END,
    total_inventory_value DESC;