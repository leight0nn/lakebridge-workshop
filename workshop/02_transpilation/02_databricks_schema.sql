/*
GlobalSupply Corp - Databricks Unity Catalog Schema Definitions
==============================================================

This script creates the Unity Catalog structure optimized for the 5 target files
from Module 1's assessment (Wave 1 & 2). The schema is designed specifically
to support the transpiled SQL workloads while leveraging Delta Lake features.

Target Files Supported:
- financial_summary.sql (4.5/10) - Basic aggregation, reporting
- order_processing.sql (5.1/10) - CRUD operations, transactions
- dynamic_reporting.sql (6.5/10) - Dynamic SQL, conditional logic
- window_functions_analysis.sql (7.2/10) - Advanced window functions
- customer_profitability.sql (7.8/10) - PIVOT operations, analytics

Business Purpose:
Create a modern, scalable data architecture that supports GlobalSupply Corp's
supply chain analytics while providing ACID guarantees and performance optimization.
*/

-- ============================================================================
-- UNITY CATALOG STRUCTURE SETUP
-- ============================================================================

-- Create main catalog for GlobalSupply Corp
CREATE CATALOG IF NOT EXISTS globalsupply_corp
COMMENT 'GlobalSupply Corp data warehouse - migrated from SQL Server';

-- Use the catalog
USE CATALOG globalsupply_corp;

-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS raw
COMMENT 'Raw data layer - equivalent to source SQL Server tables';

CREATE SCHEMA IF NOT EXISTS staging
COMMENT 'Staging area for data transformations and processing';

CREATE SCHEMA IF NOT EXISTS analytics
COMMENT 'Analytics layer - optimized views and aggregated tables';

CREATE SCHEMA IF NOT EXISTS reporting
COMMENT 'Reporting layer - final consumption-ready datasets';

-- ============================================================================
-- RAW DATA LAYER (globalsupply_corp.raw)
-- ============================================================================
-- Core TPC-H tables optimized for Delta Lake

USE SCHEMA globalsupply_corp.raw;

-- Region table
CREATE TABLE IF NOT EXISTS region (
    r_regionkey BIGINT NOT NULL,
    r_name STRING NOT NULL,
    r_comment STRING,

    -- Delta Lake optimization
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
LOCATION 's3://your-bucket/globalsupply_corp/raw/region/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Nation table
CREATE TABLE IF NOT EXISTS nation (
    n_nationkey BIGINT NOT NULL,
    n_name STRING NOT NULL,
    n_regionkey BIGINT NOT NULL,
    n_comment STRING,

    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
LOCATION 's3://your-bucket/globalsupply_corp/raw/nation/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Customer table (partitioned for performance)
CREATE TABLE IF NOT EXISTS customer (
    c_custkey BIGINT NOT NULL,
    c_name STRING NOT NULL,
    c_address STRING,
    c_nationkey BIGINT NOT NULL,
    c_phone STRING,
    c_acctbal DECIMAL(18,2),
    c_mktsegment STRING,
    c_comment STRING,

    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
PARTITIONED BY (c_mktsegment)  -- Partition by market segment for analytics queries
LOCATION 's3://your-bucket/globalsupply_corp/raw/customer/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'  -- Enable change tracking for order_processing.sql
);

-- Orders table (partitioned by date for performance)
CREATE TABLE IF NOT EXISTS orders (
    o_orderkey BIGINT NOT NULL,
    o_custkey BIGINT NOT NULL,
    o_orderstatus STRING NOT NULL,
    o_totalprice DECIMAL(18,2),
    o_orderdate DATE NOT NULL,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING,

    -- Additional columns for order processing workflow
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
PARTITIONED BY (DATE_FORMAT(o_orderdate, 'yyyy-MM'))  -- Monthly partitions
LOCATION 's3://your-bucket/globalsupply_corp/raw/orders/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days'  -- Keep history for rollbacks
);

-- Add Z-ORDER optimization for common query patterns
-- (Run after data loading)
-- OPTIMIZE globalsupply_corp.raw.orders ZORDER BY (o_orderdate, o_custkey, o_orderstatus);

-- Supplier table
CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey BIGINT NOT NULL,
    s_name STRING NOT NULL,
    s_address STRING,
    s_nationkey BIGINT NOT NULL,
    s_phone STRING,
    s_acctbal DECIMAL(18,2),
    s_comment STRING,

    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
LOCATION 's3://your-bucket/globalsupply_corp/raw/supplier/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Part table
CREATE TABLE IF NOT EXISTS part (
    p_partkey BIGINT NOT NULL,
    p_name STRING NOT NULL,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DECIMAL(18,2),
    p_comment STRING,

    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
LOCATION 's3://your-bucket/globalsupply_corp/raw/part/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- PartSupp table (relationship table with composite partitioning)
CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey BIGINT NOT NULL,
    ps_suppkey BIGINT NOT NULL,
    ps_availqty INT,
    ps_supplycost DECIMAL(18,2),
    ps_comment STRING,

    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
LOCATION 's3://your-bucket/globalsupply_corp/raw/partsupp/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'  -- Track inventory changes
);

-- LineItem table (largest table - optimized for analytics queries)
CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey BIGINT NOT NULL,
    l_partkey BIGINT NOT NULL,
    l_suppkey BIGINT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(18,2),
    l_extendedprice DECIMAL(18,2),
    l_discount DECIMAL(18,2),
    l_tax DECIMAL(18,2),
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING,

    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
PARTITIONED BY (DATE_FORMAT(l_shipdate, 'yyyy-MM'))  -- Monthly partitions by ship date
LOCATION 's3://your-bucket/globalsupply_corp/raw/lineitem/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.tuneFileSizesForRewrites' = 'true'
);

-- ============================================================================
-- ANALYTICS LAYER (globalsupply_corp.analytics)
-- ============================================================================
-- Optimized views and materialized tables for the 5 target queries

USE SCHEMA globalsupply_corp.analytics;

-- Financial Summary materialized view (supports financial_summary.sql)
CREATE MATERIALIZED VIEW IF NOT EXISTS financial_summary_mv AS
SELECT
    r.r_name AS region,
    n.n_name AS nation,
    DATE_FORMAT(o.o_orderdate, 'yyyy-MM') AS period_month,

    -- Basic metrics
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

    -- Ratios
    CASE
        WHEN SUM(l.l_extendedprice * (1 - l.l_discount)) > 0 THEN
            (SUM(l.l_extendedprice * (1 - l.l_discount)) - SUM(ps.ps_supplycost * l.l_quantity)) /
            SUM(l.l_extendedprice * (1 - l.l_discount)) * 100
        ELSE 0
    END AS gross_profit_margin_pct

FROM globalsupply_corp.raw.region r
INNER JOIN globalsupply_corp.raw.nation n ON r.r_regionkey = n.n_regionkey
INNER JOIN globalsupply_corp.raw.customer c ON n.n_nationkey = c.c_nationkey
INNER JOIN globalsupply_corp.raw.orders o ON c.c_custkey = o.o_custkey
INNER JOIN globalsupply_corp.raw.lineitem l ON o.o_orderkey = l.l_orderkey
INNER JOIN globalsupply_corp.raw.partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
INNER JOIN globalsupply_corp.raw.supplier s ON ps.ps_suppkey = s.s_suppkey

WHERE o.o_orderdate >= '2023-01-01'
    AND o.o_orderstatus IN ('F', 'O')

GROUP BY r.r_name, n.n_name, DATE_FORMAT(o.o_orderdate, 'yyyy-MM');

-- Customer analytics table (supports customer_profitability.sql and window_functions_analysis.sql)
CREATE TABLE IF NOT EXISTS customer_analytics (
    c_custkey BIGINT,
    c_name STRING,
    c_mktsegment STRING,
    analysis_date DATE,

    -- Order metrics
    total_orders BIGINT,
    total_revenue DECIMAL(18,2),
    avg_order_value DECIMAL(18,2),

    -- Time series metrics (for window functions)
    revenue_rank_in_segment BIGINT,
    revenue_percentile DECIMAL(5,4),
    months_active INT,
    first_order_date DATE,
    last_order_date DATE,

    -- Profitability metrics
    total_profit DECIMAL(18,2),
    profit_margin_pct DECIMAL(5,2),
    customer_lifetime_value DECIMAL(18,2),

    -- Update tracking
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
PARTITIONED BY (analysis_date)
LOCATION 's3://your-bucket/globalsupply_corp/analytics/customer_analytics/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Order processing metrics (supports order_processing.sql monitoring)
CREATE TABLE IF NOT EXISTS order_processing_metrics (
    report_date DATE,
    report_type STRING,

    -- Order counts by status
    open_orders BIGINT,
    processing_orders BIGINT,
    fulfilled_orders BIGINT,

    -- Financial metrics
    open_value DECIMAL(18,2),
    daily_revenue DECIMAL(18,2),

    -- Operational metrics
    avg_order_age_days DECIMAL(5,1),
    active_customers BIGINT,

    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
PARTITIONED BY (report_date)
LOCATION 's3://your-bucket/globalsupply_corp/analytics/order_processing_metrics/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- REPORTING LAYER (globalsupply_corp.reporting)
-- ============================================================================
-- Final consumption-ready views

USE SCHEMA globalsupply_corp.reporting;

-- Executive dashboard view (aggregates financial_summary results)
CREATE VIEW IF NOT EXISTS executive_financial_dashboard AS
SELECT
    region,
    SUM(gross_revenue) AS total_revenue,
    SUM(gross_profit) AS total_profit,
    AVG(gross_profit_margin_pct) AS avg_profit_margin,
    SUM(total_orders) AS total_orders,
    SUM(unique_customers) AS total_customers
FROM globalsupply_corp.analytics.financial_summary_mv
WHERE period_month >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), 'yyyy-MM')
GROUP BY region
ORDER BY total_revenue DESC;

-- Customer profitability ranking view
CREATE VIEW IF NOT EXISTS customer_profitability_ranking AS
SELECT
    c_name AS customer_name,
    c_mktsegment AS market_segment,
    total_revenue,
    total_profit,
    profit_margin_pct,
    customer_lifetime_value,
    revenue_rank_in_segment,
    CASE
        WHEN revenue_percentile >= 0.9 THEN 'Top 10%'
        WHEN revenue_percentile >= 0.75 THEN 'Top 25%'
        WHEN revenue_percentile >= 0.5 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END AS customer_tier
FROM globalsupply_corp.analytics.customer_analytics
WHERE analysis_date = (SELECT MAX(analysis_date) FROM globalsupply_corp.analytics.customer_analytics)
ORDER BY total_revenue DESC;

-- Operational dashboard view (order processing metrics)
CREATE VIEW IF NOT EXISTS operational_dashboard AS
SELECT
    report_date,
    open_orders + processing_orders + fulfilled_orders AS total_orders,
    open_orders,
    processing_orders,
    fulfilled_orders,
    daily_revenue,
    avg_order_age_days,
    active_customers,

    -- Operational KPIs
    CASE
        WHEN open_orders + processing_orders + fulfilled_orders > 0 THEN
            fulfilled_orders::FLOAT / (open_orders + processing_orders + fulfilled_orders) * 100
        ELSE 0
    END AS fulfillment_rate_pct,

    CASE
        WHEN active_customers > 0 THEN daily_revenue / active_customers
        ELSE 0
    END AS revenue_per_customer

FROM globalsupply_corp.analytics.order_processing_metrics
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY report_date DESC;

-- ============================================================================
-- PERFORMANCE OPTIMIZATION COMMANDS
-- ============================================================================

-- Z-ORDER optimization for frequently queried columns
-- Run these after data loading:

/*
-- Optimize main tables for common access patterns
OPTIMIZE globalsupply_corp.raw.orders ZORDER BY (o_orderdate, o_custkey, o_orderstatus);
OPTIMIZE globalsupply_corp.raw.lineitem ZORDER BY (l_orderkey, l_shipdate, l_partkey);
OPTIMIZE globalsupply_corp.raw.customer ZORDER BY (c_custkey, c_mktsegment, c_nationkey);
OPTIMIZE globalsupply_corp.raw.partsupp ZORDER BY (ps_partkey, ps_suppkey);

-- Vacuum old files (run periodically)
VACUUM globalsupply_corp.raw.orders RETAIN 168 HOURS;  -- 7 days
VACUUM globalsupply_corp.raw.lineitem RETAIN 168 HOURS;

-- Analyze table statistics
ANALYZE TABLE globalsupply_corp.raw.orders COMPUTE STATISTICS;
ANALYZE TABLE globalsupply_corp.raw.lineitem COMPUTE STATISTICS;
ANALYZE TABLE globalsupply_corp.raw.customer COMPUTE STATISTICS;
*/

-- ============================================================================
-- DATA LOADING EXAMPLES
-- ============================================================================

-- Example: Load data from existing SQL Server or files
/*
-- Copy data from existing tables (if migrating)
INSERT INTO globalsupply_corp.raw.customer
SELECT
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    CURRENT_TIMESTAMP() AS created_date,
    CURRENT_TIMESTAMP() AS updated_date
FROM source_database.dbo.customer;

-- Or load from files
COPY INTO globalsupply_corp.raw.customer
FROM 's3://your-bucket/migration-data/customer/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');
*/

-- ============================================================================
-- SECURITY & GOVERNANCE
-- ============================================================================

-- Grant permissions for different user groups
/*
-- Grant read access to analysts
GRANT SELECT ON SCHEMA globalsupply_corp.reporting TO `analysts`;
GRANT SELECT ON SCHEMA globalsupply_corp.analytics TO `analysts`;

-- Grant write access to ETL processes
GRANT ALL PRIVILEGES ON SCHEMA globalsupply_corp.raw TO `etl_service_principal`;
GRANT ALL PRIVILEGES ON SCHEMA globalsupply_corp.staging TO `etl_service_principal`;

-- Grant admin access to data team
GRANT ALL PRIVILEGES ON CATALOG globalsupply_corp TO `data_team`;
*/

-- ============================================================================
-- MONITORING & MAINTENANCE
-- ============================================================================

-- Create monitoring table for schema health
CREATE TABLE IF NOT EXISTS globalsupply_corp.analytics.schema_monitoring (
    check_date TIMESTAMP,
    table_name STRING,
    row_count BIGINT,
    size_gb DECIMAL(10,2),
    last_updated TIMESTAMP,
    health_status STRING,
    notes STRING
) USING DELTA
LOCATION 's3://your-bucket/globalsupply_corp/analytics/schema_monitoring/';

-- Example monitoring query
/*
INSERT INTO globalsupply_corp.analytics.schema_monitoring
SELECT
    CURRENT_TIMESTAMP() AS check_date,
    'orders' AS table_name,
    COUNT(*) AS row_count,
    ROUND(SUM(LENGTH(CAST(* AS STRING))) / 1024 / 1024 / 1024, 2) AS size_gb,
    MAX(updated_date) AS last_updated,
    CASE WHEN COUNT(*) > 0 THEN 'HEALTHY' ELSE 'WARNING' END AS health_status,
    'Daily monitoring check' AS notes
FROM globalsupply_corp.raw.orders;
*/

-- ============================================================================
-- COMPLETION STATUS
-- ============================================================================

SELECT
    '✅ GlobalSupply Corp Unity Catalog Schema Created Successfully' AS status,
    CURRENT_TIMESTAMP() AS created_at,
    'Ready for Module 2 transpiled SQL deployment' AS next_step;

/*
DEPLOYMENT CHECKLIST:
[ ] Catalog and schemas created
[ ] Core tables defined with Delta Lake optimization
[ ] Partitioning strategy implemented
[ ] Materialized views configured
[ ] Reporting layer views created
[ ] Performance optimization planned
[ ] Security permissions configured
[ ] Monitoring infrastructure ready

NEXT STEPS:
1. Load sample data for testing
2. Deploy transpiled SQL files
3. Run validation tests
4. Execute Module 3: Data Reconciliation

For production deployment, customize:
- Storage locations (S3/ADLS paths)
- Partition strategies based on data volume
- Security groups and permissions
- Monitoring and alerting thresholds
*/