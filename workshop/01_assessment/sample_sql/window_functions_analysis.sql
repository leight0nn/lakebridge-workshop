/*
Window Functions Analysis
=========================
Complexity Score: 7.2/10
Migration Hours: 12
Category: Analytics
SQL Features: Advanced Window Functions, LAG/LEAD

Business Purpose:
Advanced time series analysis and trend detection using comprehensive
window functions for supply chain analytics and forecasting.
*/

WITH order_time_series AS (
    SELECT
        c.c_custkey,
        c.c_name,
        c.c_mktsegment,
        o.o_orderkey,
        o.o_orderdate,
        o.o_totalprice,
        l.l_extendedprice * (1 - l.l_discount) AS net_line_value,

        -- Basic window functions for running calculations
        SUM(o.o_totalprice) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
            ROWS UNBOUNDED PRECEDING
        ) AS running_customer_total,

        COUNT(*) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
            ROWS UNBOUNDED PRECEDING
        ) AS customer_order_sequence,

        -- Moving averages with different window sizes
        AVG(o.o_totalprice) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_3_orders,

        AVG(o.o_totalprice) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7_orders,

        -- LAG and LEAD for trend analysis
        LAG(o.o_totalprice, 1) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
        ) AS previous_order_value,

        LAG(o.o_orderdate, 1) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
        ) AS previous_order_date,

        LEAD(o.o_totalprice, 1) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
        ) AS next_order_value,

        LEAD(o.o_orderdate, 1) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
        ) AS next_order_date,

        -- Multiple LAG values for deeper historical analysis
        LAG(o.o_totalprice, 2) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
        ) AS order_value_2_periods_ago,

        LAG(o.o_totalprice, 3) OVER (
            PARTITION BY c.c_custkey
            ORDER BY o.o_orderdate
        ) AS order_value_3_periods_ago

    FROM customer c
    INNER JOIN orders o ON c.c_custkey = o.o_custkey
    INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= '2023-01-01'
),
enhanced_analytics AS (
    SELECT
        *,
        -- Calculate periods between orders
        DATEDIFF(day, previous_order_date, o_orderdate) AS days_since_last_order,
        DATEDIFF(day, o_orderdate, next_order_date) AS days_until_next_order,

        -- Growth calculations using LAG
        CASE
            WHEN previous_order_value > 0 THEN
                ((o_totalprice - previous_order_value) / previous_order_value) * 100
            ELSE NULL
        END AS order_growth_rate_pct,

        -- Trend analysis using multiple periods
        CASE
            WHEN order_value_3_periods_ago IS NOT NULL AND order_value_2_periods_ago IS NOT NULL AND previous_order_value IS NOT NULL THEN
                CASE
                    WHEN o_totalprice > previous_order_value AND previous_order_value > order_value_2_periods_ago AND order_value_2_periods_ago > order_value_3_periods_ago THEN 'INCREASING'
                    WHEN o_totalprice < previous_order_value AND previous_order_value < order_value_2_periods_ago AND order_value_2_periods_ago < order_value_3_periods_ago THEN 'DECREASING'
                    ELSE 'FLUCTUATING'
                END
            ELSE 'INSUFFICIENT_DATA'
        END AS trend_direction,

        -- Volatility indicator using moving averages
        ABS(o_totalprice - moving_avg_3_orders) / NULLIF(moving_avg_3_orders, 0) * 100 AS volatility_from_avg_pct,

        -- First and last value functions
        FIRST_VALUE(o_totalprice) OVER (
            PARTITION BY c_custkey
            ORDER BY o_orderdate
            ROWS UNBOUNDED PRECEDING
        ) AS first_order_value,

        LAST_VALUE(o_totalprice) OVER (
            PARTITION BY c_custkey
            ORDER BY o_orderdate
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_order_value,

        -- Ranking and percentile functions
        RANK() OVER (
            PARTITION BY c_mktsegment
            ORDER BY o_totalprice DESC
        ) AS segment_order_rank,

        DENSE_RANK() OVER (
            PARTITION BY YEAR(o_orderdate), MONTH(o_orderdate)
            ORDER BY o_totalprice DESC
        ) AS monthly_dense_rank,

        PERCENT_RANK() OVER (
            PARTITION BY c_mktsegment
            ORDER BY o_totalprice
        ) AS segment_percentile_rank,

        CUME_DIST() OVER (
            PARTITION BY c_mktsegment
            ORDER BY o_totalprice
        ) AS cumulative_distribution,

        -- NTILE for quartile analysis
        NTILE(4) OVER (
            PARTITION BY c_mktsegment
            ORDER BY o_totalprice
        ) AS value_quartile,

        NTILE(10) OVER (
            PARTITION BY c_mktsegment
            ORDER BY o_totalprice
        ) AS value_decile

    FROM order_time_series
),
seasonal_pattern_analysis AS (
    SELECT
        ea.*,
        -- Seasonal analysis using window functions
        AVG(o_totalprice) OVER (
            PARTITION BY c_custkey, MONTH(o_orderdate)
            ORDER BY YEAR(o_orderdate)
        ) AS monthly_avg_by_customer,

        LAG(o_totalprice, 12) OVER (
            PARTITION BY c_custkey
            ORDER BY o_orderdate
        ) AS same_month_previous_year,

        -- Year-over-year growth
        CASE
            WHEN LAG(o_totalprice, 12) OVER (PARTITION BY c_custkey ORDER BY o_orderdate) > 0 THEN
                ((o_totalprice - LAG(o_totalprice, 12) OVER (PARTITION BY c_custkey ORDER BY o_orderdate)) /
                 LAG(o_totalprice, 12) OVER (PARTITION BY c_custkey ORDER BY o_orderdate)) * 100
            ELSE NULL
        END AS yoy_growth_rate,

        -- Rolling standard deviation for volatility measurement
        STDEV(o_totalprice) OVER (
            PARTITION BY c_custkey
            ORDER BY o_orderdate
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS rolling_6_order_std_dev,

        -- Complex window frame for business cycles
        AVG(o_totalprice) OVER (
            PARTITION BY c_mktsegment
            ORDER BY o_orderdate
            RANGE BETWEEN INTERVAL '3' MONTH PRECEDING AND INTERVAL '3' MONTH FOLLOWING
        ) AS seasonal_segment_avg

    FROM enhanced_analytics ea
),
customer_lifecycle_analysis AS (
    SELECT
        spa.*,
        -- Customer lifecycle stage analysis
        CASE customer_order_sequence
            WHEN 1 THEN 'NEW'
            WHEN 2 THEN 'SECOND_ORDER'
            ELSE
                CASE
                    WHEN customer_order_sequence <= 5 THEN 'EARLY_STAGE'
                    WHEN customer_order_sequence <= 10 THEN 'DEVELOPING'
                    WHEN customer_order_sequence <= 20 THEN 'MATURE'
                    ELSE 'LOYAL'
                END
        END AS lifecycle_stage,

        -- Advanced pattern recognition
        CASE
            WHEN trend_direction = 'INCREASING' AND order_growth_rate_pct > 10 THEN 'ACCELERATING_GROWTH'
            WHEN trend_direction = 'INCREASING' AND order_growth_rate_pct BETWEEN 0 AND 10 THEN 'STEADY_GROWTH'
            WHEN trend_direction = 'DECREASING' AND order_growth_rate_pct < -10 THEN 'DECLINING'
            WHEN trend_direction = 'DECREASING' AND order_growth_rate_pct BETWEEN -10 AND 0 THEN 'SLIGHT_DECLINE'
            WHEN trend_direction = 'FLUCTUATING' THEN 'VOLATILE'
            ELSE 'STABLE'
        END AS customer_trajectory,

        -- Risk indicators using window functions
        CASE
            WHEN days_since_last_order > 90 AND trend_direction = 'DECREASING' THEN 'HIGH_CHURN_RISK'
            WHEN days_since_last_order > 60 AND volatility_from_avg_pct > 50 THEN 'MEDIUM_CHURN_RISK'
            WHEN days_since_last_order > 30 THEN 'LOW_CHURN_RISK'
            ELSE 'ACTIVE'
        END AS churn_risk_category

    FROM seasonal_pattern_analysis spa
)
SELECT
    c_name AS customer_name,
    c_mktsegment AS market_segment,
    o_orderdate AS order_date,
    CAST(o_totalprice AS DECIMAL(18,2)) AS order_value,
    customer_order_sequence,
    lifecycle_stage,
    customer_trajectory,
    churn_risk_category,

    -- Key metrics
    CAST(running_customer_total AS DECIMAL(18,2)) AS customer_lifetime_value,
    CAST(moving_avg_3_orders AS DECIMAL(18,2)) AS recent_avg_order_value,
    CAST(order_growth_rate_pct AS DECIMAL(5,2)) AS growth_rate_pct,
    days_since_last_order,
    trend_direction,

    -- Rankings and percentiles
    segment_order_rank,
    value_quartile,
    CAST(segment_percentile_rank * 100 AS DECIMAL(5,2)) AS segment_percentile,
    CAST(cumulative_distribution * 100 AS DECIMAL(5,2)) AS cumulative_dist_pct,

    -- Seasonal indicators
    CAST(yoy_growth_rate AS DECIMAL(5,2)) AS year_over_year_growth,
    CAST(rolling_6_order_std_dev AS DECIMAL(10,2)) AS order_volatility,

    -- Advanced analytics flags
    CASE WHEN volatility_from_avg_pct > 30 THEN 'HIGH_VOLATILITY' ELSE NULL END AS volatility_flag,
    CASE WHEN segment_percentile_rank > 0.9 THEN 'TOP_10_PERCENT' ELSE NULL END AS performance_flag

FROM customer_lifecycle_analysis
WHERE customer_order_sequence >= 2  -- Focus on repeat customers
ORDER BY c_name, o_orderdate;