/*
DATABRICKS SQL VERSION - Customer Profitability Analysis
========================================================
Original: customer_profitability.sql
Complexity Score: 7.8/10 (Medium)
Migration Wave: Wave 2 - Standard Migration
Migration Hours: 18

TRANSPILATION CHANGES MADE:
- Replaced DATEDIFF(day, date1, date2) with DATEDIFF(date2, date1)
- Replaced GETDATE() with CURRENT_DATE()
- Converted STRING_AGG to ARRAY_JOIN(COLLECT_LIST())
- Added Unity Catalog table references
- Optimized PIVOT operation for Databricks SQL
- Enhanced window functions for better performance

VALIDATION STATUS: ⚠️ Requires testing - complex analytics logic
PERFORMANCE NOTES: Consider materialized views for repeated calculations
*/

-- Databricks SQL version optimized for Delta Lake and Spark
WITH customer_base_metrics AS (
    SELECT
        c.c_custkey,
        c.c_name,
        c.c_acctbal,
        c.c_mktsegment,
        n.n_name AS customer_nation,
        r.r_name AS customer_region,

        -- RFM Analysis components (adjusted for Databricks)
        MAX(o.o_orderdate) AS last_order_date,
        COUNT(DISTINCT o.o_orderkey) AS frequency_orders,
        AVG(o.o_totalprice) AS avg_order_value,
        SUM(o.o_totalprice) AS total_monetary_value,

        -- Temporal analysis (date function adjustments)
        DATEDIFF(CURRENT_DATE(), MAX(o.o_orderdate)) AS recency_days,
        DATEDIFF(MAX(o.o_orderdate), MIN(o.o_orderdate)) AS customer_lifespan_days,

        -- Product diversity
        COUNT(DISTINCT l.l_partkey) AS unique_products_purchased,
        COUNT(DISTINCT l.l_suppkey) AS suppliers_engaged,

        -- Order patterns (window function works the same)
        AVG(DATEDIFF(o.o_orderdate, LAG(o.o_orderdate) OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate))) AS avg_days_between_orders

    FROM globalsupply_corp.raw.customer c
    INNER JOIN globalsupply_corp.raw.orders o ON c.c_custkey = o.o_custkey
    INNER JOIN globalsupply_corp.raw.lineitem l ON o.o_orderkey = l.l_orderkey
    INNER JOIN globalsupply_corp.raw.nation n ON c.c_nationkey = n.n_nationkey
    INNER JOIN globalsupply_corp.raw.region r ON n.n_regionkey = r.r_regionkey
    WHERE o.o_orderdate >= '2022-01-01'
    GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_mktsegment, n.n_name, r.r_name
),
rfm_scoring AS (
    SELECT
        *,
        -- RFM Scoring using NTILE for quintile analysis
        NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,  -- Lower recency days = higher score
        NTILE(5) OVER (ORDER BY frequency_orders) AS frequency_score,
        NTILE(5) OVER (ORDER BY total_monetary_value) AS monetary_score,

        -- Customer lifetime value calculation
        CASE
            WHEN customer_lifespan_days > 0 THEN
                (total_monetary_value / customer_lifespan_days) * 365
            ELSE total_monetary_value
        END AS estimated_annual_value,

        -- Customer segmentation based on behavior
        CASE
            WHEN recency_days <= 30 AND frequency_orders >= 5 THEN 'Champions'
            WHEN recency_days <= 60 AND frequency_orders >= 3 THEN 'Loyal Customers'
            WHEN recency_days <= 90 AND total_monetary_value >= 1000 THEN 'Potential Loyalists'
            WHEN recency_days > 180 THEN 'At Risk'
            WHEN frequency_orders = 1 THEN 'New Customers'
            ELSE 'Developing'
        END AS customer_segment

    FROM customer_base_metrics
),
product_preferences AS (
    SELECT
        c.c_custkey,
        -- String aggregation converted to Databricks syntax
        ARRAY_JOIN(COLLECT_LIST(DISTINCT p.p_brand), ', ') AS preferred_brands,
        ARRAY_JOIN(COLLECT_LIST(DISTINCT p.p_type), '|') AS product_types,

        -- Top product by spend
        FIRST_VALUE(p.p_name) OVER (
            PARTITION BY c.c_custkey
            ORDER BY SUM(l.l_extendedprice * (1 - l.l_discount)) DESC
        ) AS top_product_by_spend,

        MAX(SUM(l.l_extendedprice * (1 - l.l_discount))) OVER (
            PARTITION BY c.c_custkey
        ) AS top_product_spend

    FROM globalsupply_corp.raw.customer c
    INNER JOIN globalsupply_corp.raw.orders o ON c.c_custkey = o.o_custkey
    INNER JOIN globalsupply_corp.raw.lineitem l ON o.o_orderkey = l.l_orderkey
    INNER JOIN globalsupply_corp.raw.part p ON l.l_partkey = p.p_partkey
    WHERE o.o_orderdate >= '2022-01-01'
    GROUP BY c.c_custkey, p.p_name, p.p_brand, p.p_type
),
quarterly_performance AS (
    SELECT
        c.c_custkey,
        YEAR(o.o_orderdate) AS order_year,
        QUARTER(o.o_orderdate) AS order_quarter,
        SUM(o.o_totalprice) AS quarterly_revenue,
        COUNT(DISTINCT o.o_orderkey) AS quarterly_orders,

        -- Growth calculation using window functions
        LAG(SUM(o.o_totalprice)) OVER (
            PARTITION BY c.c_custkey
            ORDER BY YEAR(o.o_orderdate), QUARTER(o.o_orderdate)
        ) AS prev_quarter_revenue,

        CASE
            WHEN LAG(SUM(o.o_totalprice)) OVER (
                PARTITION BY c.c_custkey
                ORDER BY YEAR(o.o_orderdate), QUARTER(o.o_orderdate)
            ) > 0 THEN
                (SUM(o.o_totalprice) - LAG(SUM(o.o_totalprice)) OVER (
                    PARTITION BY c.c_custkey
                    ORDER BY YEAR(o.o_orderdate), QUARTER(o.o_orderdate)
                )) / LAG(SUM(o.o_totalprice)) OVER (
                    PARTITION BY c.c_custkey
                    ORDER BY YEAR(o.o_orderdate), QUARTER(o.o_orderdate)
                ) * 100
            ELSE NULL
        END AS quarter_over_quarter_growth_pct

    FROM globalsupply_corp.raw.customer c
    INNER JOIN globalsupply_corp.raw.orders o ON c.c_custkey = o.o_custkey
    WHERE o.o_orderdate >= '2022-01-01'
    GROUP BY c.c_custkey, YEAR(o.o_orderdate), QUARTER(o.o_orderdate)
),
-- PIVOT operation converted to Databricks SQL syntax
quarterly_revenue_pivot AS (
    SELECT
        c_custkey,
        SUM(CASE WHEN order_year = 2022 AND order_quarter = 1 THEN quarterly_revenue ELSE 0 END) AS q1_2022,
        SUM(CASE WHEN order_year = 2022 AND order_quarter = 2 THEN quarterly_revenue ELSE 0 END) AS q2_2022,
        SUM(CASE WHEN order_year = 2022 AND order_quarter = 3 THEN quarterly_revenue ELSE 0 END) AS q3_2022,
        SUM(CASE WHEN order_year = 2022 AND order_quarter = 4 THEN quarterly_revenue ELSE 0 END) AS q4_2022,
        SUM(CASE WHEN order_year = 2023 AND order_quarter = 1 THEN quarterly_revenue ELSE 0 END) AS q1_2023,
        SUM(CASE WHEN order_year = 2023 AND order_quarter = 2 THEN quarterly_revenue ELSE 0 END) AS q2_2023,
        SUM(CASE WHEN order_year = 2023 AND order_quarter = 3 THEN quarterly_revenue ELSE 0 END) AS q3_2023,
        SUM(CASE WHEN order_year = 2023 AND order_quarter = 4 THEN quarterly_revenue ELSE 0 END) AS q4_2023,

        -- Growth trends
        AVG(quarter_over_quarter_growth_pct) AS avg_growth_rate

    FROM quarterly_performance
    GROUP BY c_custkey
),
customer_risk_indicators AS (
    SELECT
        r.c_custkey,
        r.customer_segment,
        r.recency_days,
        r.frequency_orders,
        r.avg_days_between_orders,

        -- Risk scoring
        CASE
            WHEN r.recency_days > 365 THEN 'High Risk - Inactive'
            WHEN r.recency_days > 180 AND r.frequency_orders = 1 THEN 'High Risk - One-time buyer'
            WHEN r.avg_days_between_orders > 180 THEN 'Medium Risk - Infrequent buyer'
            WHEN r.customer_segment = 'At Risk' THEN 'Medium Risk - Declining engagement'
            ELSE 'Low Risk'
        END AS churn_risk_category,

        -- Engagement score
        CASE
            WHEN r.recency_days <= 30 THEN 100
            WHEN r.recency_days <= 90 THEN 75
            WHEN r.recency_days <= 180 THEN 50
            WHEN r.recency_days <= 365 THEN 25
            ELSE 10
        END AS engagement_score

    FROM rfm_scoring r
)
-- Final comprehensive customer profitability report
SELECT
    r.c_custkey,
    r.c_name,
    r.customer_nation,
    r.customer_region,
    r.c_mktsegment,

    -- RFM Analysis
    r.recency_score,
    r.frequency_score,
    r.monetary_score,
    CONCAT(r.recency_score, r.frequency_score, r.monetary_score) AS rfm_segment,

    -- Customer segmentation
    r.customer_segment,
    cri.churn_risk_category,
    cri.engagement_score,

    -- Financial metrics
    CAST(r.total_monetary_value AS DECIMAL(18,2)) AS lifetime_value,
    CAST(r.avg_order_value AS DECIMAL(18,2)) AS avg_order_value,
    CAST(r.estimated_annual_value AS DECIMAL(18,2)) AS estimated_annual_value,

    -- Behavioral metrics
    r.frequency_orders,
    r.recency_days,
    CAST(r.avg_days_between_orders AS DECIMAL(5,1)) AS avg_days_between_orders,
    r.unique_products_purchased,
    r.suppliers_engaged,

    -- Product preferences
    pp.preferred_brands,
    pp.product_types,
    pp.top_product_by_spend,
    CAST(pp.top_product_spend AS DECIMAL(18,2)) AS top_product_spend,

    -- Quarterly performance (PIVOT results)
    CAST(qrp.q1_2022 AS DECIMAL(18,2)) AS revenue_q1_2022,
    CAST(qrp.q2_2022 AS DECIMAL(18,2)) AS revenue_q2_2022,
    CAST(qrp.q3_2022 AS DECIMAL(18,2)) AS revenue_q3_2022,
    CAST(qrp.q4_2022 AS DECIMAL(18,2)) AS revenue_q4_2022,
    CAST(qrp.q1_2023 AS DECIMAL(18,2)) AS revenue_q1_2023,
    CAST(qrp.q2_2023 AS DECIMAL(18,2)) AS revenue_q2_2023,
    CAST(qrp.q3_2023 AS DECIMAL(18,2)) AS revenue_q3_2023,
    CAST(qrp.q4_2023 AS DECIMAL(18,2)) AS revenue_q4_2023,

    -- Growth metrics
    CAST(qrp.avg_growth_rate AS DECIMAL(5,2)) AS avg_quarterly_growth_pct,

    -- Rankings within market segment
    RANK() OVER (PARTITION BY r.c_mktsegment ORDER BY r.total_monetary_value DESC) AS segment_value_rank,
    PERCENT_RANK() OVER (PARTITION BY r.c_mktsegment ORDER BY r.total_monetary_value) AS segment_percentile

FROM rfm_scoring r
LEFT JOIN product_preferences pp ON r.c_custkey = pp.c_custkey
LEFT JOIN quarterly_revenue_pivot qrp ON r.c_custkey = qrp.c_custkey
LEFT JOIN customer_risk_indicators cri ON r.c_custkey = cri.c_custkey

WHERE r.frequency_orders >= 1  -- Include all customers with at least one order

ORDER BY r.total_monetary_value DESC, r.c_name;

/*
DATABRICKS OPTIMIZATION SUGGESTIONS:

1. Create materialized views for expensive calculations:
   CREATE MATERIALIZED VIEW customer_rfm_analysis AS
   SELECT c_custkey, recency_score, frequency_score, monetary_score, ...
   FROM (subquery with RFM calculations);

2. Use Delta Lake features for performance:
   -- Cache frequently accessed customer data
   CACHE TABLE globalsupply_corp.raw.customer;

   -- Optimize tables for customer analytics
   OPTIMIZE globalsupply_corp.raw.orders ZORDER BY (o_custkey, o_orderdate);

3. Consider partitioning customer analytics by segment:
   CREATE TABLE customer_profitability_analysis
   USING DELTA
   PARTITIONED BY (c_mktsegment)
   AS SELECT ... FROM (this query);

4. Use Spark SQL functions for better performance:
   -- Replace complex CASE statements with built-in functions where possible
   -- Use approx_percentile for large datasets

TESTING RECOMMENDATIONS:
1. Validate RFM scoring against business rules
2. Test PIVOT operation results match expected format
3. Verify string aggregation produces correct concatenated results
4. Check that window functions handle edge cases (single orders, etc.)
5. Validate growth calculations with known test data
6. Test performance with realistic data volumes

BUSINESS VALIDATION:
1. Verify customer segments align with business definitions
2. Test churn risk categories with known customer outcomes
3. Validate product preference aggregation accuracy
4. Review quarterly revenue PIVOT format with business users
*/