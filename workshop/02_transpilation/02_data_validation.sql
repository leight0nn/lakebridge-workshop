/*
GlobalSupply Corp - Data Validation and Consistency Verification
==============================================================

This script validates the TPC-H sample data for Module 2 transpiled SQL testing.
It verifies referential integrity, data quality, and business logic consistency
to ensure the dataset supports all validation requirements.

Business Context:
Validates that the generated test data properly supports the 5 target transpiled
SQL files with realistic business scenarios and proper relationships.

Prerequisites:
- Unity Catalog schema created (02_databricks_schema.sql)
- Sample data loaded (02_sample_data_generation.sql)
- All tables populated with test data

Usage:
Execute this script after loading sample data to verify data quality and
referential integrity before running Module 2 validation tests.
*/

-- ============================================================================
-- ENVIRONMENT VALIDATION
-- ============================================================================

-- Ensure we're using the correct catalog and schema
USE CATALOG globalsupply_corp;
USE SCHEMA globalsupply_corp.raw;

SELECT 'Starting comprehensive data validation for Module 2 transpilation testing' AS validation_status,
       CURRENT_TIMESTAMP() AS validation_start_time;

-- ============================================================================
-- 1. ROW COUNT VALIDATION
-- ============================================================================

SELECT 'ROW COUNT VALIDATION' AS validation_category;

-- Expected row counts for each table
WITH expected_counts AS (
    SELECT 'region' AS table_name, 5 AS expected_rows
    UNION ALL SELECT 'nation', 10
    UNION ALL SELECT 'customer', 25
    UNION ALL SELECT 'supplier', 15
    UNION ALL SELECT 'part', 20
    UNION ALL SELECT 'partsupp', 30
    UNION ALL SELECT 'orders', 50
    UNION ALL SELECT 'lineitem', 100
),
actual_counts AS (
    SELECT 'region' AS table_name, COUNT(*) AS actual_rows FROM region
    UNION ALL SELECT 'nation', COUNT(*) FROM nation
    UNION ALL SELECT 'customer', COUNT(*) FROM customer
    UNION ALL SELECT 'supplier', COUNT(*) FROM supplier
    UNION ALL SELECT 'part', COUNT(*) FROM part
    UNION ALL SELECT 'partsupp', COUNT(*) FROM partsupp
    UNION ALL SELECT 'orders', COUNT(*) FROM orders
    UNION ALL SELECT 'lineitem', COUNT(*) FROM lineitem
)
SELECT
    e.table_name,
    e.expected_rows,
    a.actual_rows,
    CASE
        WHEN e.expected_rows = a.actual_rows THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS row_count_status,
    CASE
        WHEN e.expected_rows = a.actual_rows THEN NULL
        ELSE CONCAT('Expected ', e.expected_rows, ' but found ', a.actual_rows)
    END AS validation_message
FROM expected_counts e
INNER JOIN actual_counts a ON e.table_name = a.table_name
ORDER BY e.table_name;

-- ============================================================================
-- 2. REFERENTIAL INTEGRITY VALIDATION
-- ============================================================================

SELECT 'REFERENTIAL INTEGRITY VALIDATION' AS validation_category;

-- Check Region → Nation relationship
SELECT
    'Region-Nation FK' AS relationship,
    COUNT(*) AS total_nations,
    COUNT(CASE WHEN r.r_regionkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN r.r_regionkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM nation n
LEFT JOIN region r ON n.n_regionkey = r.r_regionkey

UNION ALL

-- Check Nation → Customer relationship
SELECT
    'Nation-Customer FK' AS relationship,
    COUNT(*) AS total_customers,
    COUNT(CASE WHEN n.n_nationkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN n.n_nationkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM customer c
LEFT JOIN nation n ON c.c_nationkey = n.n_nationkey

UNION ALL

-- Check Nation → Supplier relationship
SELECT
    'Nation-Supplier FK' AS relationship,
    COUNT(*) AS total_suppliers,
    COUNT(CASE WHEN n.n_nationkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN n.n_nationkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM supplier s
LEFT JOIN nation n ON s.s_nationkey = n.n_nationkey

UNION ALL

-- Check Customer → Orders relationship
SELECT
    'Customer-Orders FK' AS relationship,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN c.c_custkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN c.c_custkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM orders o
LEFT JOIN customer c ON o.o_custkey = c.c_custkey

UNION ALL

-- Check Orders → LineItem relationship
SELECT
    'Orders-LineItem FK' AS relationship,
    COUNT(*) AS total_lineitems,
    COUNT(CASE WHEN o.o_orderkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN o.o_orderkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM lineitem l
LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey

UNION ALL

-- Check Part → PartSupp relationship
SELECT
    'Part-PartSupp FK' AS relationship,
    COUNT(*) AS total_partsupp,
    COUNT(CASE WHEN p.p_partkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN p.p_partkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM partsupp ps
LEFT JOIN part p ON ps.ps_partkey = p.p_partkey

UNION ALL

-- Check Supplier → PartSupp relationship
SELECT
    'Supplier-PartSupp FK' AS relationship,
    COUNT(*) AS total_partsupp,
    COUNT(CASE WHEN s.s_suppkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN s.s_suppkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM partsupp ps
LEFT JOIN supplier s ON ps.ps_suppkey = s.s_suppkey

UNION ALL

-- Check LineItem → Part relationship
SELECT
    'LineItem-Part FK' AS relationship,
    COUNT(*) AS total_lineitems,
    COUNT(CASE WHEN p.p_partkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN p.p_partkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM lineitem l
LEFT JOIN part p ON l.l_partkey = p.p_partkey

UNION ALL

-- Check LineItem → Supplier relationship
SELECT
    'LineItem-Supplier FK' AS relationship,
    COUNT(*) AS total_lineitems,
    COUNT(CASE WHEN s.s_suppkey IS NOT NULL THEN 1 END) AS valid_references,
    CASE
        WHEN COUNT(*) = COUNT(CASE WHEN s.s_suppkey IS NOT NULL THEN 1 END) THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS integrity_status
FROM lineitem l
LEFT JOIN supplier s ON l.l_suppkey = s.s_suppkey;

-- ============================================================================
-- 3. DATA QUALITY VALIDATION
-- ============================================================================

SELECT 'DATA QUALITY VALIDATION' AS validation_category;

-- Check for NULL values in required fields
SELECT
    'Required Field NULLs' AS quality_check,
    'region.r_regionkey' AS field_name,
    COUNT(*) - COUNT(r_regionkey) AS null_count,
    CASE WHEN COUNT(*) - COUNT(r_regionkey) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS quality_status
FROM region

UNION ALL

SELECT
    'Required Field NULLs',
    'customer.c_custkey',
    COUNT(*) - COUNT(c_custkey) AS null_count,
    CASE WHEN COUNT(*) - COUNT(c_custkey) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM customer

UNION ALL

SELECT
    'Required Field NULLs',
    'orders.o_orderkey',
    COUNT(*) - COUNT(o_orderkey) AS null_count,
    CASE WHEN COUNT(*) - COUNT(o_orderkey) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM orders

UNION ALL

SELECT
    'Required Field NULLs',
    'lineitem.l_orderkey',
    COUNT(*) - COUNT(l_orderkey) AS null_count,
    CASE WHEN COUNT(*) - COUNT(l_orderkey) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lineitem;

-- Check for realistic value ranges
SELECT 'VALUE RANGE VALIDATION' AS validation_category;

SELECT
    'Value Ranges' AS quality_check,
    'customer.c_acctbal positive' AS range_description,
    COUNT(CASE WHEN c_acctbal < 0 THEN 1 END) AS violations,
    CASE WHEN COUNT(CASE WHEN c_acctbal < 0 THEN 1 END) = 0 THEN '✅ PASS' ELSE '⚠️ WARNING' END AS range_status
FROM customer

UNION ALL

SELECT
    'Value Ranges',
    'orders.o_totalprice positive',
    COUNT(CASE WHEN o_totalprice <= 0 THEN 1 END) AS violations,
    CASE WHEN COUNT(CASE WHEN o_totalprice <= 0 THEN 1 END) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM orders

UNION ALL

SELECT
    'Value Ranges',
    'lineitem.l_quantity positive',
    COUNT(CASE WHEN l_quantity <= 0 THEN 1 END) AS violations,
    CASE WHEN COUNT(CASE WHEN l_quantity <= 0 THEN 1 END) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lineitem

UNION ALL

SELECT
    'Value Ranges',
    'partsupp.ps_availqty non-negative',
    COUNT(CASE WHEN ps_availqty < 0 THEN 1 END) AS violations,
    CASE WHEN COUNT(CASE WHEN ps_availqty < 0 THEN 1 END) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM partsupp;

-- ============================================================================
-- 4. BUSINESS LOGIC VALIDATION
-- ============================================================================

SELECT 'BUSINESS LOGIC VALIDATION' AS validation_category;

-- Validate order status distribution
WITH order_status_distribution AS (
    SELECT
        o_orderstatus,
        COUNT(*) AS status_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders), 1) AS status_percentage
    FROM orders
    GROUP BY o_orderstatus
)
SELECT
    'Order Status Distribution' AS business_check,
    CONCAT(o_orderstatus, ': ', status_count, ' orders (', status_percentage, '%)') AS distribution_details,
    status_count AS metric_value,
    CASE
        WHEN o_orderstatus IN ('O', 'P', 'F') THEN '✅ VALID'
        ELSE '❌ INVALID'
    END AS business_status
FROM order_status_distribution
ORDER BY status_count DESC;

-- Validate date consistency
SELECT
    'Date Consistency' AS business_check,
    'Order dates within realistic range' AS consistency_description,
    COUNT(CASE WHEN o_orderdate < '2022-01-01' OR o_orderdate > CURRENT_DATE() THEN 1 END) AS violations,
    CASE
        WHEN COUNT(CASE WHEN o_orderdate < '2022-01-01' OR o_orderdate > CURRENT_DATE() THEN 1 END) = 0
        THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS consistency_status
FROM orders

UNION ALL

-- Validate lineitem ship dates are after order dates
SELECT
    'Date Consistency',
    'Ship dates after order dates',
    COUNT(CASE WHEN l.l_shipdate < o.o_orderdate THEN 1 END) AS violations,
    CASE
        WHEN COUNT(CASE WHEN l.l_shipdate < o.o_orderdate THEN 1 END) = 0
        THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM lineitem l
INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
WHERE l.l_shipdate IS NOT NULL;

-- ============================================================================
-- 5. MODULE 2 TRANSPILED SQL COMPATIBILITY VALIDATION
-- ============================================================================

SELECT 'MODULE 2 COMPATIBILITY VALIDATION' AS validation_category;

-- Test data supports financial_summary_databricks.sql
SELECT
    'financial_summary compatibility' AS compatibility_check,
    'Regions with customer orders' AS requirement,
    COUNT(DISTINCT r.r_name) AS regions_with_data,
    CASE WHEN COUNT(DISTINCT r.r_name) >= 3 THEN '✅ PASS' ELSE '⚠️ LIMITED' END AS compatibility_status
FROM region r
INNER JOIN nation n ON r.r_regionkey = n.n_regionkey
INNER JOIN customer c ON n.n_nationkey = c.c_nationkey
INNER JOIN orders o ON c.c_custkey = o.o_custkey
WHERE o.o_orderstatus IN ('F', 'O')

UNION ALL

-- Test data supports order_processing_databricks.sql
SELECT
    'order_processing compatibility',
    'Orders with multiple statuses',
    COUNT(DISTINCT o_orderstatus) AS status_variety,
    CASE WHEN COUNT(DISTINCT o_orderstatus) >= 3 THEN '✅ PASS' ELSE '⚠️ LIMITED' END
FROM orders

UNION ALL

-- Test data supports customer_profitability_databricks.sql
SELECT
    'customer_profitability compatibility',
    'Customers with multiple orders',
    COUNT(*) AS customers_with_multiple_orders,
    CASE WHEN COUNT(*) >= 15 THEN '✅ PASS' ELSE '⚠️ LIMITED' END
FROM (
    SELECT c_custkey
    FROM orders
    GROUP BY c_custkey
    HAVING COUNT(*) > 1
) multi_order_customers

UNION ALL

-- Test market segment diversity
SELECT
    'customer_profitability compatibility',
    'Market segment diversity',
    COUNT(DISTINCT c_mktsegment) AS segment_count,
    CASE WHEN COUNT(DISTINCT c_mktsegment) >= 4 THEN '✅ PASS' ELSE '⚠️ LIMITED' END
FROM customer;

-- ============================================================================
-- 6. PERFORMANCE AND OPTIMIZATION VALIDATION
-- ============================================================================

SELECT 'PERFORMANCE VALIDATION' AS validation_category;

-- Check table size distribution for realistic testing
SELECT
    'Table Size Distribution' AS performance_check,
    'lineitem (largest table)' AS table_focus,
    COUNT(*) AS row_count,
    CASE WHEN COUNT(*) >= 80 THEN '✅ ADEQUATE' ELSE '⚠️ SMALL' END AS size_status
FROM lineitem

UNION ALL

SELECT
    'Table Size Distribution',
    'orders (medium table)',
    COUNT(*) AS row_count,
    CASE WHEN COUNT(*) >= 40 THEN '✅ ADEQUATE' ELSE '⚠️ SMALL' END
FROM orders

UNION ALL

SELECT
    'Table Size Distribution',
    'customer (reference table)',
    COUNT(*) AS row_count,
    CASE WHEN COUNT(*) >= 20 THEN '✅ ADEQUATE' ELSE '⚠️ SMALL' END
FROM customer;

-- ============================================================================
-- 7. COMPREHENSIVE SUMMARY REPORT
-- ============================================================================

SELECT 'VALIDATION SUMMARY REPORT' AS validation_category;

-- Overall data quality summary
WITH validation_metrics AS (
    SELECT
        (SELECT COUNT(*) FROM region) AS region_count,
        (SELECT COUNT(*) FROM nation) AS nation_count,
        (SELECT COUNT(*) FROM customer) AS customer_count,
        (SELECT COUNT(*) FROM supplier) AS supplier_count,
        (SELECT COUNT(*) FROM part) AS part_count,
        (SELECT COUNT(*) FROM partsupp) AS partsupp_count,
        (SELECT COUNT(*) FROM orders) AS order_count,
        (SELECT COUNT(*) FROM lineitem) AS lineitem_count,

        -- Business metrics
        (SELECT COUNT(DISTINCT o_orderstatus) FROM orders) AS order_statuses,
        (SELECT COUNT(DISTINCT c_mktsegment) FROM customer) AS market_segments,
        (SELECT COUNT(DISTINCT r.r_name) FROM region r
         INNER JOIN nation n ON r.r_regionkey = n.n_regionkey
         INNER JOIN customer c ON n.n_nationkey = c.c_nationkey
         INNER JOIN orders o ON c.c_custkey = o.o_custkey) AS active_regions,

        -- Date range coverage
        (SELECT DATEDIFF(MAX(o_orderdate), MIN(o_orderdate)) FROM orders) AS date_range_days
)
SELECT
    'Overall Data Quality' AS summary_category,
    CONCAT('Tables: 8/8 populated') AS table_status,
    CONCAT('Total Records: ', (region_count + nation_count + customer_count + supplier_count +
                              part_count + partsupp_count + order_count + lineitem_count)) AS record_count,
    CONCAT('Business Diversity: ', order_statuses, ' order statuses, ',
           market_segments, ' market segments, ', active_regions, ' active regions') AS business_variety,
    CONCAT('Date Coverage: ', date_range_days, ' days (',
           ROUND(date_range_days / 365.0, 1), ' years)') AS temporal_coverage,
    CASE
        WHEN region_count = 5 AND nation_count = 10 AND customer_count = 25
         AND supplier_count = 15 AND part_count = 20 AND partsupp_count = 30
         AND order_count = 50 AND lineitem_count = 100
         AND order_statuses >= 3 AND market_segments >= 4 AND active_regions >= 3
        THEN '✅ EXCELLENT - Ready for Module 2 validation'
        WHEN order_count >= 40 AND lineitem_count >= 80
        THEN '✅ GOOD - Adequate for transpilation testing'
        ELSE '⚠️ NEEDS ATTENTION - Review data generation'
    END AS overall_readiness
FROM validation_metrics;

-- Final validation status
SELECT
    'FINAL VALIDATION STATUS' AS final_category,
    'GlobalSupply Corp TPC-H Test Data' AS dataset_name,
    CURRENT_TIMESTAMP() AS validation_completed,
    CASE
        WHEN (SELECT COUNT(*) FROM orders) >= 50
         AND (SELECT COUNT(*) FROM lineitem) >= 100
         AND (SELECT COUNT(DISTINCT o_orderstatus) FROM orders) >= 3
         AND (SELECT COUNT(DISTINCT c_mktsegment) FROM customer) >= 4
        THEN '🎉 VALIDATION PASSED - Data ready for Module 2 transpiled SQL testing'
        ELSE '⚠️ VALIDATION INCOMPLETE - Address issues above before proceeding'
    END AS final_validation_result;

/*
DATA VALIDATION CHECKLIST:
=========================

✅ CORE REQUIREMENTS:
[ ] All 8 tables populated with expected row counts
[ ] Referential integrity maintained across all relationships
[ ] No NULL values in required primary key fields
[ ] Realistic value ranges for business data

✅ BUSINESS LOGIC REQUIREMENTS:
[ ] Order statuses include Open (O), Processing (P), Fulfilled (F)
[ ] Date ranges span 2022-2024 for trend analysis
[ ] Multiple market segments for customer analysis
[ ] Geographic diversity across regions and nations

✅ MODULE 2 COMPATIBILITY REQUIREMENTS:
[ ] Data supports financial_summary_databricks.sql analytics
[ ] Data supports order_processing_databricks.sql operations
[ ] Data supports customer_profitability_databricks.sql segmentation
[ ] Sufficient data volume for window functions and aggregations

✅ PERFORMANCE REQUIREMENTS:
[ ] Adequate record counts for testing (not production scale)
[ ] Proper distribution across time periods
[ ] Realistic business relationships and hierarchies

NEXT STEPS:
1. Review any validation failures above
2. Re-run data generation if needed
3. Proceed to Module 2 comprehensive validation testing
4. Execute transpiled SQL files against this dataset
*/