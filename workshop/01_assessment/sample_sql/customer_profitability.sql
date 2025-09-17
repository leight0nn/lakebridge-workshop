/*
Customer Profitability Analysis
==============================
Complexity Score: 7.8/10
Migration Hours: 18
Category: Analytics
SQL Features: PIVOT, Window Functions, String Aggregation

Business Purpose:
Comprehensive customer profitability analysis with RFM segmentation,
lifetime value calculations, and growth trend analysis.
*/

WITH customer_base_metrics AS (
    SELECT
        c.c_custkey,
        c.c_name,
        c.c_acctbal,
        c.c_mktsegment,
        n.n_name AS customer_nation,
        r.r_name AS customer_region,

        -- RFM Analysis components
        MAX(o.o_orderdate) AS last_order_date,
        COUNT(DISTINCT o.o_orderkey) AS frequency_orders,
        AVG(o.o_totalprice) AS avg_order_value,
        SUM(o.o_totalprice) AS total_monetary_value,

        -- Temporal analysis
        DATEDIFF(day, MAX(o.o_orderdate), GETDATE()) AS recency_days,
        DATEDIFF(day, MIN(o.o_orderdate), MAX(o.o_orderdate)) AS customer_lifespan_days,

        -- Product diversity
        COUNT(DISTINCT l.l_partkey) AS unique_products_purchased,
        COUNT(DISTINCT l.l_suppkey) AS suppliers_engaged,

        -- Order patterns
        AVG(DATEDIFF(day, LAG(o.o_orderdate) OVER (PARTITION BY c.c_custkey ORDER BY o.o_orderdate), o.o_orderdate)) AS avg_days_between_orders

    FROM customer c
    INNER JOIN orders o ON c.c_custkey = o.o_custkey
    INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    INNER JOIN nation n ON c.c_nationkey = n.n_nationkey
    INNER JOIN region r ON n.n_regionkey = r.r_regionkey
    WHERE o.o_orderdate >= '2022-01-01'
    GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_mktsegment, n.n_name, r.r_name
),
rfm_scoring AS (
    SELECT
        *,
        -- RFM Scores (1-5 scale)
        CASE
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END AS recency_score,

        CASE NTILE(5) OVER (ORDER BY frequency_orders DESC)
            WHEN 1 THEN 5 WHEN 2 THEN 4 WHEN 3 THEN 3 WHEN 4 THEN 2 ELSE 1
        END AS frequency_score,

        CASE NTILE(5) OVER (ORDER BY total_monetary_value DESC)
            WHEN 1 THEN 5 WHEN 2 THEN 4 WHEN 3 THEN 3 WHEN 4 THEN 2 ELSE 1
        END AS monetary_score

    FROM customer_base_metrics
),
customer_segments AS (
    SELECT
        *,
        -- Combined RFM segment
        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Cannot Lose Them'
            WHEN recency_score <= 1 AND frequency_score <= 1 THEN 'Lost Customers'
            ELSE 'Others'
        END AS rfm_segment,

        -- Customer lifetime value prediction (simplified)
        CASE
            WHEN customer_lifespan_days > 0 AND avg_days_between_orders > 0 THEN
                (total_monetary_value / customer_lifespan_days) *
                (365 * 2) * -- Projected 2 years
                (1 / (avg_days_between_orders / 30.0)) -- Order frequency factor
            ELSE total_monetary_value
        END AS predicted_clv

    FROM rfm_scoring
),
-- PIVOT for yearly revenue analysis
yearly_revenue_pivot AS (
    SELECT
        c_custkey,
        c_name,
        c_mktsegment,
        customer_region,
        ISNULL([2022], 0) AS revenue_2022,
        ISNULL([2023], 0) AS revenue_2023,
        ISNULL([2024], 0) AS revenue_2024
    FROM (
        SELECT
            c.c_custkey,
            c.c_name,
            c.c_mktsegment,
            cs.customer_region,
            YEAR(o.o_orderdate) AS order_year,
            SUM(o.o_totalprice) AS yearly_revenue
        FROM customer c
        INNER JOIN orders o ON c.c_custkey = o.o_custkey
        INNER JOIN customer_segments cs ON c.c_custkey = cs.c_custkey
        WHERE YEAR(o.o_orderdate) BETWEEN 2022 AND 2024
        GROUP BY c.c_custkey, c.c_name, c.c_mktsegment, cs.customer_region, YEAR(o.o_orderdate)
    ) AS source_table
    PIVOT (
        SUM(yearly_revenue)
        FOR order_year IN ([2022], [2023], [2024])
    ) AS pivot_table
),
growth_analysis AS (
    SELECT
        yrp.*,
        cs.rfm_segment,
        cs.predicted_clv,
        cs.unique_products_purchased,
        cs.recency_days,

        -- Growth calculations
        CASE
            WHEN yrp.revenue_2022 > 0 THEN
                ((yrp.revenue_2023 - yrp.revenue_2022) / yrp.revenue_2022) * 100
            ELSE NULL
        END AS growth_2022_to_2023,

        CASE
            WHEN yrp.revenue_2023 > 0 THEN
                ((yrp.revenue_2024 - yrp.revenue_2023) / yrp.revenue_2023) * 100
            ELSE NULL
        END AS growth_2023_to_2024,

        -- Total customer value
        yrp.revenue_2022 + yrp.revenue_2023 + yrp.revenue_2024 AS total_3_year_value

    FROM yearly_revenue_pivot yrp
    INNER JOIN customer_segments cs ON yrp.c_custkey = cs.c_custkey
),
segment_summary AS (
    SELECT
        customer_region,
        c_mktsegment,
        rfm_segment,
        COUNT(*) AS customer_count,
        AVG(predicted_clv) AS avg_predicted_clv,
        AVG(total_3_year_value) AS avg_3_year_value,
        AVG(growth_2022_to_2023) AS avg_growth_2022_2023,
        AVG(growth_2023_to_2024) AS avg_growth_2023_2024,
        AVG(unique_products_purchased) AS avg_product_diversity,

        -- Top customers in segment using STRING_AGG
        STRING_AGG(
            CASE
                WHEN total_3_year_value > (
                    SELECT AVG(total_3_year_value) * 1.5
                    FROM growth_analysis ga2
                    WHERE ga2.customer_region = ga.customer_region
                    AND ga2.c_mktsegment = ga.c_mktsegment
                    AND ga2.rfm_segment = ga.rfm_segment
                )
                THEN ga.c_name
                ELSE NULL
            END,
            ', '
        ) WITHIN GROUP (ORDER BY ga.total_3_year_value DESC) AS top_performers,

        -- Risk indicators
        SUM(CASE WHEN recency_days > 90 THEN 1 ELSE 0 END) AS at_risk_count

    FROM growth_analysis ga
    GROUP BY customer_region, c_mktsegment, rfm_segment
)
SELECT
    customer_region,
    c_mktsegment AS market_segment,
    rfm_segment,
    customer_count,
    CAST(avg_predicted_clv AS DECIMAL(18,2)) AS avg_predicted_clv,
    CAST(avg_3_year_value AS DECIMAL(18,2)) AS avg_3_year_value,
    CAST(avg_growth_2022_2023 AS DECIMAL(5,2)) AS avg_growth_2022_2023,
    CAST(avg_growth_2023_2024 AS DECIMAL(5,2)) AS avg_growth_2023_2024,
    CAST(avg_product_diversity AS DECIMAL(5,1)) AS avg_product_diversity,
    at_risk_count,
    CAST((at_risk_count * 100.0 / customer_count) AS DECIMAL(5,2)) AS at_risk_percentage,
    top_performers,

    -- Segment prioritization score
    CASE
        WHEN rfm_segment = 'Champions' THEN 10
        WHEN rfm_segment = 'Loyal Customers' THEN 9
        WHEN rfm_segment = 'Cannot Lose Them' THEN 8
        WHEN rfm_segment = 'At Risk' THEN 7
        WHEN rfm_segment = 'New Customers' THEN 6
        ELSE 5
    END AS priority_score

FROM segment_summary
WHERE customer_count >= 5  -- Only meaningful segments
ORDER BY priority_score DESC, avg_3_year_value DESC;